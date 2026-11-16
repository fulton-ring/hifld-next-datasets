"""Focused tests for the USGS US Landslide Regions draft pipeline."""
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse, urlsplit

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.us_landslide_regions import (
    us_landslide_regions,
    us_landslide_regions_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import us_landslide_regions as src
from dagster_hifld.sources.esri_geometry import to_wkb

FIXTURE_COUNT = 8
FIXTURE_OBJECTIDS = (2, 3, 5, 8, 13, 21, 34, 55)


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
            "extent": {**src.EXPECTED_LAYER_EXTENT,
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
    import dagster_hifld.assets.us_landslide_regions as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_IDS_SHA256", ""),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_INVALID_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_TOTAL_VERTEX_COUNT", None),
        patch.object(src, "EXPECTED_RINGS_PER_FEATURE_DISTRIBUTION", None),
        patch.object(src, "EXPECTED_OBSERVED_BOUNDS", None),
        patch.object(src, "PAGE_START_RECORDS", segment_size),
        patch.object(src, "PAGE_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(asset_module, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(asset_module, "EXPECTED_INVALID_GEOMETRY_COUNT", None),
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


class UsLandslideRegionsSourceTests(unittest.TestCase):
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
        self.assertEqual(src.EXPECTED_COUNT, 680_497)
        self.assertEqual(src.MAX_RECORD_COUNT, 2000)
        self.assertEqual(src.PAGE_START_RECORDS, 1000)
        self.assertEqual(src.EXPECTED_SERVICE_DESCRIPTION,
                         "USGS US Landslide version 3 polygons")
        self.assertTrue(src.EXPECTED_IDS_SHA256)
        self.assertTrue(src.EXPECTED_SCHEMA_SHA256)
        self.assertTrue(src.EXPECTED_RECORD_SHA256)
        self.assertTrue(src.EXPECTED_GEOMETRY_SHA256)
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        self.assertEqual(src.EXPECTED_ATTRIBUTE_NAMES,
                         ("OBJECTID", "fid_1", "USGS_ID", "Date_Min", "Date_Max",
                          "Fatalities", "Confidence", "LS_Type", "Inventory",
                          "Inv_URL", "Info_Sourc", "Notes", "Lat_N", "Lon_W",
                          "Shape__Area", "Shape__Length"))
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.FILE_SLUG]
                            for key in keys))
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
        self.assertTrue(src._page_matches_slice(
            json.dumps(page).encode(), 0, 4, roster))
        self.assertFalse(src._page_matches_slice(
            json.dumps({"error": {}}).encode(), 0, 4, roster))
        self.assertFalse(src._page_matches_slice(
            json.dumps({"features": rows[:2]}).encode(), 0, 4, roster))
        self.assertFalse(src._page_matches_slice(
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
                (capture_root / f"{name}.wire").write_bytes(b"wire")
                receipt = {"method": "GET", "url": "https://x/y", "final_url": "https://x/y",
                           "complete_body": True, "status": 200, "bytes": len(body),
                           "sha256": src._sha(body), "wire_bytes": 4,
                           "wire_sha256": src._sha(b"wire"), "observed_at": "t"}
                (capture_root / f"{name}.receipt.json").write_bytes(
                    json.dumps(receipt).encode())
            gaps = src._plan_page_gaps(capture_root, roster)
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
                        [us_landslide_regions, us_landslide_regions_complete],
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
                    candidate = result.output_for_node(
                        "ingest__us_landslide_regions__us_landslide_regions")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_blocking_check_passes_on_reuse_with_pinned_gates(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                src.capture_us_landslide_regions(
                    storage, version="pin-reuse-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json")
                                      .read_bytes())
                import dagster_hifld.assets.us_landslide_regions as asset_module
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT",
                                  baseline["source_null_geometry_count"]), \
                        patch.object(src, "EXPECTED_INVALID_GEOMETRY_COUNT",
                                     baseline["source_invalid_geometry_count"]), \
                        patch.object(asset_module, "EXPECTED_NULL_GEOMETRY_COUNT",
                                     baseline["source_null_geometry_count"]), \
                        patch.object(asset_module, "EXPECTED_INVALID_GEOMETRY_COUNT",
                                     baseline["source_invalid_geometry_count"]):
                    second = src.capture_us_landslide_regions(
                        storage, version="pin-reuse-run-2",
                        capture_directory=str(capture_root))
                    self.assertTrue(second["reused"])
                    self.assertEqual(second["source_invalid_geometry_count"],
                                     baseline["source_invalid_geometry_count"])
                    self.assertEqual(second["source_null_geometry_count"],
                                     baseline["source_null_geometry_count"])
                    instance_dir = root / "dagster"
                    instance_dir.mkdir()
                    (instance_dir / "dagster.yaml").write_text(
                        "telemetry:\n  enabled: false\n")
                    from dagster import DagsterInstance, materialize
                    with DagsterInstance.from_config(str(instance_dir)) as instance:
                        result = materialize(
                            [us_landslide_regions, us_landslide_regions_complete],
                            resources={"staging_storage": storage},
                            instance=instance,
                            run_config={"ops": {
                                "ingest__us_landslide_regions__us_landslide_regions":
                                    {"config": {"capture_directory":
                                                str(capture_root)}}}},
                        )
                        self.assertTrue(result.success)
                        checks = {check.check_name: check
                                  for check in result.get_asset_check_evaluations()}
                        self.assertTrue(checks["us_landslide_regions_complete"].passed)
                        candidate = result.output_for_node(
                            "ingest__us_landslide_regions__us_landslide_regions")
                        self.assertTrue(candidate["reused"])
                        self.assertEqual(candidate["source_invalid_geometry_count"],
                                         baseline["source_invalid_geometry_count"])

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
            self.assertEqual(audit["null_geometry_count"], 1)
            self.assertEqual(audit["rings_per_feature_distribution"],
                             {str(ring_count): count
                              for ring_count, count in {1: 5, 2: 2}.items()})
            self.assertTrue(audit["baseline_pending_complete_capture"])
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
                       "extent": {**src.EXPECTED_LAYER_EXTENT, "xmax": 1.0,
                                  "spatialReference": {**src.NATIVE_LAYER_SR}}}),
            "source_id_population_drift": dict(ids=list(FIXTURE_OBJECTIDS)[:-1]),
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

    def test_geometry_gates_are_enforced_once_pinned(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                src.capture_us_landslide_regions(
                    _staging(root), version="gate-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                                "source_null_geometry_count_drift"):
                        src.fetch_us_landslide_regions_pages(capture_root)
                with patch.object(src, "EXPECTED_INVALID_GEOMETRY_COUNT", 5):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                                "source_invalid_geometry_count_drift"):
                        src.fetch_us_landslide_regions_pages(capture_root)
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    with patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                                      baseline["source_geometry_sha256"]):
                        with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT",
                                          baseline["source_null_geometry_count"]):
                            with patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT",
                                              baseline["source_empty_geometry_count"]):
                                with patch.object(src, "EXPECTED_INVALID_GEOMETRY_COUNT",
                                                  baseline["source_invalid_geometry_count"]):
                                    with patch.object(src, "EXPECTED_TOTAL_VERTEX_COUNT",
                                                      baseline["source_total_vertex_count"]):
                                        with patch.object(
                                                src,
                                                "EXPECTED_RINGS_PER_FEATURE_DISTRIBUTION",
                                                {int(key): value for key, value in
                                                 baseline["source_rings_per_feature_distribution"].items()}):
                                            with patch.object(
                                                    src, "EXPECTED_OBSERVED_BOUNDS",
                                                    baseline["observed_bounds"]):
                                                again = src.capture_us_landslide_regions(
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
                src.capture_us_landslide_regions(
                    storage, version="pin-run", capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                                "source_id_roster_drift"):
                        src.capture_us_landslide_regions(
                            storage, version="pin-run-drift",
                            capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                     .read_bytes())
                self.assertTrue(failure["reason_code"].startswith("source_id_roster_drift"))


if __name__ == "__main__":
    unittest.main()
