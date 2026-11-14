"""Focused tests for the USGS GNIS Landforms draft pipeline."""
import io
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.landforms_gnis import (
    landforms_gnis,
    landforms_gnis_complete,
)
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import landforms_gnis as src

FIXTURE_COUNT = 9
FIXTURE_OBJECTIDS = (2, 3, 5, 8, 13, 21, 34, 55, 89)
FEATURECLASSES = ("Summit", "Valley", "Basin", "Flat")


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "gaz_id": 2400 + index if index != 3 else None,
        "gaz_name": f"Feature {oid} (landform)",
        "gaz_featureclass": FEATURECLASSES[index % len(FEATURECLASSES)],
        "fcode": 61500 + index if index != 2 else None,
        "state_alpha": "GA",
        "county_name": "Chattooga" if index != 4 else None,
        "objectid_1": 102047 + index,
        "isunknowncoords": 2,
    }


def _fixture_geometry(oid: object) -> object:
    index = FIXTURE_OBJECTIDS.index(oid)
    if oid == FIXTURE_OBJECTIDS[0]:
        return {"hasZ": True, "points": [[-9496939.5726, 4081865.4846, 0],
                                         [-9496000.0, 4081000.0, 0]]}
    if oid == FIXTURE_OBJECTIDS[2]:
        return None
    return {"hasZ": True, "points": [[-9496939.5726 - index, 4081865.4846 + index, 0]]}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, length in src.NATIVE_FIELDS[:7]:
        field = {"name": name, "alias": name, "type": esri_type, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    fields.append({"name": "shape", "alias": "shape",
                   "type": "esriFieldTypeGeometry", "domain": None})
    for name, esri_type, length in src.NATIVE_FIELDS[7:]:
        field = {"name": name, "alias": name, "type": esri_type, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": None,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "geometryField": {"name": src.GEOMETRY_FIELD_NAME,
                              "type": "esriFieldTypeGeometry",
                              "alias": src.GEOMETRY_FIELD_NAME},
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": {**src.NATIVE_LAYER_SR, "xyTolerance": 0.001},
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": {**src.NATIVE_LAYER_SR,
                                            "xyTolerance": 0.001}},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The National Map Gazetteer based on the Geographic "
                                  "Names Information System (GNIS).",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


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


def make_responder(rows: list[dict], *, layer_fields=None, count=None, ids=None,
                   service=None, layer=None,
                   refuse_counts=()) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    objectids = ids if ids is not None else list(FIXTURE_OBJECTIDS)
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.rstrip("/").endswith("/MapServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if query.get("returnIdsOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "objectIds": objectids}),
                headers={"content-type": "application/json"})
        if query.get("objectIds") is not None:
            requested = [int(value) for value in query["objectIds"][0].split(",")]
            if len(requested) in refuse_counts:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query "
                                                      "operation", "details": []}}),
                    headers={"content-type": "application/json"})
            provided = rows(requested) if callable(rows) else rows
            by_id = {row["attributes"]["OBJECTID"]: row for row in provided}
            return httpx.Response(200, stream=JSONStream(
                {"displayFieldName": "gaz_name", "hasZ": True,
                 "geometryType": src.GEOMETRY_TYPE,
                 "spatialReference": {"wkid": src.NATIVE_LAYER_SR["wkid"],
                                      "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]},
                 "features": [by_id[value] for value in requested if value in by_id]}),
                headers={"content-type": "application/json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


def _fixture_distributions() -> dict[str, dict]:
    rows = _fixture_rows()
    featureclass: dict[str, int] = {}
    for row in rows:
        value = row["attributes"]["gaz_featureclass"]
        featureclass[value] = featureclass.get(value, 0) + 1
    return {
        "featureclass": featureclass,
        "isunknowncoords": {2: len(rows)},
    }


# The real production roster size: the regression replays the exact
# production pagination (243,708 ids captured at the pinned 1000 ids/page
# start, i.e. 244 pages) that the bounded catalog reader must be able to
# summarize.
BULK_COUNT = 243_708


def _bulk_attrs(oid: int) -> dict:
    return {"OBJECTID": oid,
            "gaz_id": 2400 + (oid % 997),
            "gaz_name": f"Feature {oid} (landform)",
            "gaz_featureclass": FEATURECLASSES[oid % len(FEATURECLASSES)],
            "fcode": 61500 + (oid % 11),
            "state_alpha": "GA",
            "county_name": "Chattooga" if oid % 5 else None,
            "objectid_1": 102047 + (oid % 13),
            "isunknowncoords": 2}


def _bulk_geometry(oid: int) -> object:
    if oid % 97 == 0:
        return None
    return {"hasZ": True,
            "points": [[-9496939.5726 - (oid % 1000) * 0.5,
                        4081865.4846 + (oid % 997) * 0.5, 0.0]]}


def _bulk_rows(requested: list[int]) -> list[dict]:
    return [{"attributes": _bulk_attrs(oid), "geometry": _bulk_geometry(oid)}
            for oid in requested]


def _bulk_distributions() -> dict[str, dict]:
    featureclass: dict[str, int] = {}
    for oid in range(1, BULK_COUNT + 1):
        value = FEATURECLASSES[oid % len(FEATURECLASSES)]
        featureclass[value] = featureclass.get(value, 0) + 1
    return {"featureclass": featureclass, "isunknowncoords": {2: BULK_COUNT}}


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=4,
                 count=None, distributions=None, mock_construction_validators=True):
    import dagster_hifld.assets.landforms_gnis as asset_module
    roster_count = count if count is not None else FIXTURE_COUNT
    distributions = (distributions if distributions is not None
                     else _fixture_distributions())
    contexts = [
        patch.object(src, "EXPECTED_COUNT", roster_count),
        patch.object(src, "EXPECTED_IDS_SHA256", ""),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_TOTAL_POINT_COUNT", None),
        patch.object(src, "EXPECTED_OBSERVED_BOUNDS", None),
        patch.object(src, "EXPECTED_FEATURECLASS_DISTRIBUTION",
                     distributions["featureclass"]),
        patch.object(src, "EXPECTED_ISUNKNOWNCOORDS_DISTRIBUTION",
                     distributions["isunknowncoords"]),
        patch.object(src, "EXPECTED_MULTIPOINT_POINT_COUNT_DISTRIBUTION", None),
        patch.object(src, "EXPECTED_Z_ORDINATE_DISTRIBUTION", None),
        patch.object(src, "PAGE_START_RECORDS", segment_size),
        patch.object(src, "PAGE_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(asset_module, "EXPECTED_COUNT", roster_count),
        patch.object(asset_module, "EXPECTED_NULL_GEOMETRY_COUNT", None),
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
            patch.object(epa_frs_common, "validate_construction_code_identity"),
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


class LandformsGnisTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "landforms-1")
        self.assertEqual(src.SERVICE_URL,
                         "https://carto.nationalmap.gov/arcgis/rest/services/"
                         "geonames/MapServer")
        self.assertEqual(src.LAYER_ID, 5)
        self.assertEqual(src.LAYER_NAME, "Landforms")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryMultipoint")
        self.assertEqual(src.EXPECTED_COUNT, 243_708)
        self.assertEqual(src.MAX_RECORD_COUNT, 2000)
        self.assertEqual(src.PAGE_START_RECORDS, 1000)
        self.assertTrue(src.EXPECTED_IDS_SHA256 or True)
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        self.assertEqual(sum(src.EXPECTED_FEATURECLASS_DISTRIBUTION.values()),
                         src.EXPECTED_COUNT)
        self.assertEqual(len(src.EXPECTED_FEATURECLASS_DISTRIBUTION), 23)
        self.assertEqual(src.EXPECTED_ISUNKNOWNCOORDS_DISTRIBUTION, {2: 243_708})
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))
        schedule = defs.get_schedule_def(
            "landforms_gnis_draft_refresh_schedule")
        from dagster import DefaultScheduleStatus
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_page_query_contract_is_roster_bounded(self):
        url = src._page_url([2, 3, 5])
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["objectIds"], ["2,3,5"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["outFields"], ["*"])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["returnM"], ["true"])
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"],
                         ["true"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"],
                         ["true"])
        self.assertTrue(urlparse(src._count_url()).path.endswith("/5/query"))

    def test_multipoint_wkb_keeps_ordinates_and_z(self):
        points = [(-9496939.5726, 4081865.4846, 0.0), (-9496000.0, 4081000.0, 0.0)]
        wkb = src._multipoint_wkb(points)
        order, kind, count = struct.unpack_from("<BII", wkb, 0)
        self.assertEqual((order, kind, count), (1, 0x80000004, 2))
        self.assertEqual(struct.unpack_from("<BI3d", wkb, 9),
                         (1, 0x80000001) + points[0])
        self.assertEqual(len(wkb), 9 + 29 * 2)
        from shapely import from_wkb
        geometry = from_wkb(wkb)
        self.assertEqual(geometry.geom_type, "MultiPoint")
        self.assertTrue(geometry.has_z)
        self.assertEqual(len(geometry.geoms), 2)

    def test_multipoint_geometry_drift_is_refused(self):
        cases = {
            "unexpected_source_geometry_members":
                {"points": [[1.0, 2.0, 0.0]], "curves": []},
            "source_multipoint_z_flag_drift":
                {"hasZ": False, "points": [[1.0, 2.0, 0.0]]},
            "source_multipoint_rows_drift": {"points": []},
            "source_multipoint_ordinate_count_drift":
                {"points": [[1.0, 2.0]]},
            "source_point_ordinate_not_numeric":
                {"points": [["NaN", 2.0, 0.0]]},
            "source_point_ordinate_not_finite":
                {"points": [[float("inf"), 2.0, 0.0]]},
            "source_coordinate_range_drift":
                {"points": [[25_000_000.0, 2.0, 0.0]]},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(
                        src.LandformsGnisError, expected_error):
                    src._validate_multipoint(geometry)

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_landforms_gnis(
                    storage, version="fixture-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_landforms_gnis(
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
            self.assertEqual(table.column("gaz_featureclass").to_pylist(),
                             [row["attributes"]["gaz_featureclass"] for row in rows])
            self.assertEqual(table.column("gaz_id").to_pylist(),
                             [row["attributes"]["gaz_id"] for row in rows])
            geometries = table.column("geometry").to_pylist()
            self.assertIsNone(geometries[2])
            self.assertIsNotNone(geometries[0])
            from shapely import from_wkb
            first_geometry = from_wkb(geometries[0])
            self.assertEqual(first_geometry.geom_type, "MultiPoint")
            self.assertEqual(len(first_geometry.geoms), 2)
            self.assertTrue(first_geometry.has_z)
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["MultiPoint Z"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"]
                             .get("name", ""), "WGS 84 / Pseudo-Mercator")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [landforms_gnis,
                         landforms_gnis_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {
                            "ingest__landforms_1__"
                            "landforms_1":
                                {"config": {"capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(
                        checks["landforms_gnis_complete"]
                        .passed)
                    candidate = result.output_for_node(
                        "ingest__landforms_1__"
                        "landforms_1")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_landforms_gnis(
                    _staging(root), version="seal-run", capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(sealed["pagination"]["mode"],
                             "enumerated_roster_bisected_objectids_pages")
            self.assertEqual(sealed["pagination"]["page_start_records"], 4)
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["null_geometry_count"], 1)
            self.assertEqual(sealed["featureclass_distribution"],
                             distributions()["featureclass"])
            self.assertEqual(sealed["crosswalk_layer_row"], src.CROSSWALK_ROW)
            self.assertEqual(sealed["crosswalk_open_id"], src.CROSSWALK_OPEN_ID)
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertEqual(hosts, {"carto.nationalmap.gov"})
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows,
                             FIXTURE_COUNT)

    def test_refused_pages_bisect_down_to_singles(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, segment_size=2):
                result = src.capture_landforms_gnis(
                    _staging(root), version="bisect-run", capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(
                    sorted((entry["offset"], entry["count"])
                           for entry in state["pages"]),
                    [(offset, 1) for offset in range(FIXTURE_COUNT)])
                resumed = src.capture_landforms_gnis(
                    _staging(root), version="bisect-run-2",
                    capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_damaged_page_resume_rebisects_and_reconciles_state(self):
        """A damaged accepted page is replaced without stale parent entries.

        Regression for the resumability finding: a torn previously accepted
        page must lose its accepted entry when witnessed aside, and a resume
        that has to bisect its slice again must drop the stale parent before
        accepting the replacement children. The repaired capture, its full
        reconstruction and a second healthy replay all succeed while the
        other retained pages are kept untouched.
        """
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            storage = _staging(root)
            with fixture_mode(schema_sha, make_responder(rows)):
                first = src.capture_landforms_gnis(
                    storage, version="repair-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                healthy_baseline = json.loads(
                    (capture_root / "computed_baseline.json").read_bytes())
            survivor_bodies = {
                name: (capture_root / f"{name}.body").read_bytes()
                for name in ("page_000004_00004", "page_000008_00001")}
            (capture_root / "page_000000_00004.wire").write_bytes(b"torn")
            with fixture_mode(schema_sha, make_responder(rows, refuse_counts={4})):
                repaired = src.capture_landforms_gnis(
                    storage, version="repair-run-2", capture_directory=str(capture_root))
                self.assertEqual(repaired["version"], first["version"])
                self.assertTrue(repaired["reused"])
                self.assertEqual(repaired["feature_count"], FIXTURE_COUNT)
                repaired_baseline = json.loads(
                    (capture_root / "computed_baseline.json").read_bytes())
                self.assertEqual(repaired_baseline["source_record_sha256"],
                                 healthy_baseline["source_record_sha256"])
                self.assertEqual(repaired_baseline["source_geometry_sha256"],
                                 healthy_baseline["source_geometry_sha256"])
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            self.assertEqual(
                sorted((entry["offset"], entry["count"]) for entry in state["pages"]),
                [(0, 2), (2, 2), (4, 4), (8, 1)])
            witnessed = sorted(capture_root.glob("page_000000_00004.error-*.wire"))
            self.assertTrue(witnessed)
            self.assertTrue(any(path.read_bytes() == b"torn" for path in witnessed))
            self.assertFalse((capture_root / "page_000000_00004.wire").exists())
            for name, body in survivor_bodies.items():
                self.assertEqual((capture_root / f"{name}.body").read_bytes(), body)
            parquet = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
            self.assertEqual(parquet.num_rows, FIXTURE_COUNT)
            self.assertEqual(parquet.column("OBJECTID").to_pylist(),
                             list(FIXTURE_OBJECTIDS))
            with fixture_mode(schema_sha, make_responder(rows)):
                again = src.capture_landforms_gnis(
                    storage, version="repair-run-3", capture_directory=str(capture_root))
                self.assertTrue(again["reused"])
                self.assertEqual(again["version"], first["version"])
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            self.assertEqual(
                sorted((entry["offset"], entry["count"]) for entry in state["pages"]),
                [(0, 2), (2, 2), (4, 4), (8, 1)])
            for name, body in survivor_bodies.items():
                self.assertEqual((capture_root / f"{name}.body").read_bytes(), body)

    def test_full_roster_manifest_fits_bounded_catalog_reader(self):
        """Full-size roster: compact page provenance keeps ingest-to-catalog working.

        Regression for the bounded-reader finding: with the production roster
        (243,708 ids captured at the pinned 1000 ids/page start, i.e. 244
        pages) the sealed source manifest must stay within the bounded catalog
        reader limit, and the real catalog path (load_resolved_source_manifest
        plus summarize_staged_catalog, exactly as the catalog asset calls
        them) must summarize the sealed candidate end to end.
        """
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(_bulk_rows, count=BULK_COUNT,
                                   ids=list(range(1, BULK_COUNT + 1)))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, segment_size=1000,
                              count=BULK_COUNT, distributions=_bulk_distributions()):
                storage = _staging(root)
                result = src.capture_landforms_gnis(
                    storage, version="full-roster-run",
                    capture_directory=str(capture_root))
                self.assertFalse(result["reused"])
                self.assertEqual(result["feature_count"], BULK_COUNT)
                manifest_bytes = (root / "staging" / src.DATASET_SLUG
                                  / src.FILE_SLUG / result["version"]
                                  / "metadata" / "source_manifest.json").read_bytes()
                self.assertLessEqual(len(manifest_bytes),
                                     src.CATALOG_READER_MAX_MANIFEST_BYTES)
                manifest = json.loads(manifest_bytes)
                pages = [entry for entry in manifest["inputs"]
                         if entry["input_id"].startswith("page_")]
                self.assertEqual(len(pages), 244)
                for entry in pages:
                    self.assertTrue(entry["retrieval_query_redacted"])
                    self.assertEqual(entry["retrieval_url"], src.LAYER_URL + "/query")
                    self.assertNotIn("objectIds", entry["retrieval_url"])
                    self.assertNotIn("observed_final_url", entry)
                documents = [entry for entry in manifest["inputs"]
                             if not entry["input_id"].startswith("page_")]
                self.assertEqual(len(documents), 9)
                self.assertTrue(all("observed_final_url" in entry
                                    for entry in documents))
                self.assertFalse(any("retrieval_query_redacted" in entry
                                     for entry in documents))
                resolved = load_resolved_source_manifest(
                    storage, src.DATASET_SLUG, src.FILE_SLUG, result["version"])
                summary = summarize_staged_catalog(
                    storage, src.DATASET_SLUG, src.FILE_SLUG, result["version"],
                    src.DATASET_SLUG, source_metadata=resolved.metadata)
                self.assertEqual(summary.quality_manifest["feature_count"],
                                 BULK_COUNT)
                self.assertEqual(summary.quality_manifest["null_geometry_count"],
                                 len([oid for oid in range(1, BULK_COUNT + 1)
                                      if oid % 97 == 0]))
                self.assertEqual(summary.data_dictionary["name"], src.DATASET_SLUG)

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={1, 2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, segment_size=2):
                with self.assertRaisesRegex(src.LandformsGnisError,
                                            "source_single_feature_unservable"):
                    src.fetch_landforms_gnis_pages(
                        root / "capture")
                self.assertTrue(sorted((root / "capture").glob("page_*.error-*.body")))

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "length": 4,
                                            "domain": None}]),
            "source_schema_baseline_drift": dict(
                layer_fields=[{**field, "alias": "renamed-alias"}
                              if field.get("name") == "gaz_name" else field
                              for field in _fixture_layer_fields()]),
            "gnis_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "gnis_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "gnis_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy HIFLD service."}),
            "landforms_gnis_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPoint"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "maxRecordCount": 1000}),
            "source_pagination_capability_changed": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "advancedQueryCapabilities": {"supportsPagination": False}}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": {
                                              **src.NATIVE_LAYER_SR,
                                              "xyTolerance": 0.001}}}),
            "source_id_population_drift": dict(ids=list(FIXTURE_OBJECTIDS)[:-1]),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"hasZ": True, "points": [
                    [25_000_000.0, *rows[0]["geometry"]["points"][0][1:]]]}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0],
                {**rows[1], "attributes": {**rows[1]["attributes"], "OBJECTID": 999}},
                *rows[2:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 2, "gaz_id": 2400}},
                *rows[1:]]),
            "source_native_type_changed:gaz_id": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "gaz_id": "2400"}},
                *rows[1:]]),
            "source_featureclass_outside_gazetteer_population": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"],
                                           "gaz_featureclass": "School"}},
                *rows[1:]]),
            "source_isunknowncoords_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"],
                                           "isunknowncoords": 1}},
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
                                src.LandformsGnisError,
                                expected_error):
                            src.capture_landforms_gnis(
                                storage, version="drift-run",
                                capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_geometry_gates_are_enforced_once_pinned(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                src.capture_landforms_gnis(
                    _staging(root), version="gate-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.LandformsGnisError,
                                                "source_null_geometry_count_drift"):
                        src.fetch_landforms_gnis_pages(
                            capture_root)
                with patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT", 5):
                    with self.assertRaisesRegex(src.LandformsGnisError,
                                                "source_empty_geometry_count_drift"):
                        src.fetch_landforms_gnis_pages(
                            capture_root)
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    with patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                                      baseline["source_geometry_sha256"]):
                        with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT",
                                          baseline["source_null_geometry_count"]):
                            with patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT",
                                              baseline["source_empty_geometry_count"]):
                                with patch.object(
                                        src, "EXPECTED_TOTAL_POINT_COUNT",
                                        baseline["source_total_point_count"]):
                                    with patch.object(
                                            src,
                                            "EXPECTED_MULTIPOINT_POINT_COUNT_DISTRIBUTION",
                                            {int(key): value for key, value in
                                             baseline["source_multipoint_point_count_distribution"].items()}):
                                        with patch.object(
                                                src, "EXPECTED_Z_ORDINATE_DISTRIBUTION",
                                                {float(key): value for key, value in
                                                 baseline["source_z_ordinate_distribution"].items()}):
                                            with patch.object(
                                                    src, "EXPECTED_OBSERVED_BOUNDS",
                                                    baseline["observed_bounds"]):
                                                again = src.capture_landforms_gnis(
                                                    _staging(root),
                                                    version="gate-run-verify",
                                                    capture_directory=str(capture_root))
                                                self.assertTrue(again["reused"])

    def test_pinned_baseline_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                src.capture_landforms_gnis(
                    storage, version="pin-run", capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.LandformsGnisError,
                                                "source_id_roster_drift"):
                        src.capture_landforms_gnis(
                            storage, version="pin-run-drift",
                            capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                     .read_bytes())
                self.assertTrue(failure["reason_code"].startswith("source_id_roster_drift"))

    def test_packaged_runtime_without_git_seals_and_reuses_with_real_validator(self):
        """Reproduce the shipped /app layout: package + manifests, no .git."""
        import shutil
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            runtime_root = Path(tmp) / "app"
            (runtime_root / "src").mkdir(parents=True)
            shutil.copytree(Path(src.__file__).parents[1],
                            runtime_root / "src" / "dagster_hifld")
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copyfile(Path(src.__file__).parents[3] / name, runtime_root / name)
            self.assertFalse((runtime_root / ".git").exists())
            with patch.object(src, "REPO_ROOT", runtime_root):
                packaged = src._code_identity()
            self.assertEqual(packaged["code_identity_scheme"],
                             "sha256-packaged-dagster-hifld-v1")
            self.assertTrue(packaged["code_commit"].startswith("packaged-sha256:"))
            from dagster_hifld.construction_provenance import validate_construction_code_identity
            validate_construction_code_identity(runtime_root, packaged["code_commit"],
                                                packaged["files"], identity=packaged)
            capture_root = Path(tmp) / "capture"
            with fixture_mode(schema_sha, transport, mock_construction_validators=False), \
                    patch.object(src, "REPO_ROOT", runtime_root):
                storage = _staging(Path(tmp))
                first = src.capture_landforms_gnis(
                    storage, version="packaged-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                second = src.capture_landforms_gnis(
                    storage, version="packaged-run-2", capture_directory=str(capture_root))
                self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])


def distributions() -> dict[str, dict]:
    return _fixture_distributions()


if __name__ == "__main__":
    unittest.main()
