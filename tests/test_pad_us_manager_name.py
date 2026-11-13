"""Focused tests for the USGS PAD-US Manager Name draft pipeline."""
import io
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.pad_us_manager_name import (
    pad_us_manager_name,
    pad_us_manager_name_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import pad_us_manager_name as src

FIXTURE_COUNT = 7
FIXTURE_OBJECTIDS = [2, 3, 7, 12, 41, 58, 900]  # sparse, non-contiguous roster (ascending)


def _fixture_attrs(oid: object) -> dict:
    values = {
        "OBJECTID": oid,
        "FeatClass": "Fee",
        "Category": "Fee",
        "Own_Type": "FED",
        "Own_Name": "USFS",
        "Loc_Own": "",
        "Mang_Type": "FED",
        "Mang_Name": "USFS",
        "Loc_Mang": "",
        "Des_Tp": "",
        "Loc_Ds": "",
        "Unit_Nm": "Pike National Forest",
        "Loc_Nm": "",
        "State_Nm": "Colorado",
        "Agg_Src": "Gap",
        "GIS_Src": "sample.gis",
        "Src_Date": "2023-06-01",
        "GIS_Acres": 123,
        "Source_PAID": "SAMPLE_PAID",
        "WDPA_Cd": None,
        "Pub_Access": None,
        "Access_Src": None,
        "Access_Dt": None,
        "GAP_Sts": "1",
        "GAPCdSrc": None,
        "GAPCdDt": None,
        "IUCN_Cat": "Ib",
        "IUCNCtSrc": None,
        "IUCNCtDt": None,
        "Date_Est": None,
        "Comments": None,
        "EsmtHldr": None,
        "EHoldTyp": None,
        "WebServiceMangName": "USFS",
        "TranspPct": None,
        "Shape__Area": 1000.5,
        "Shape__Length": 500.25,
    }
    return dict(values)


def _cw_square(x0: float, y0: float, size: float) -> list:
    return [[x0, y0], [x0 + size, y0], [x0 + size, y0 - size], [x0, y0 - size], [x0, y0]]


def _ccw_square(x0: float, y0: float, size: float) -> list:
    return [[x0, y0], [x0, y0 - size], [x0 + size, y0 - size], [x0 + size, y0], [x0, y0]]


def _fixture_geometry(oid: object) -> object:
    index = FIXTURE_OBJECTIDS.index(oid)
    if index == 0:
        return {"rings": [_cw_square(0.0, 0.0, 1.0)]}
    if index == 1:
        return {"rings": [_cw_square(0.0, 0.0, 1.0), _ccw_square(0.25, -0.5, 0.25)]}
    if index == 2:
        return {"rings": [_cw_square(0.0, 0.0, 1.0), _cw_square(2.0, 0.0, 1.0)]}
    if index == 3:
        return None
    if index == 4:
        return {"rings": [[["NaN", "NaN"], [1.0, 0.0], [1.0, -1.0], [0.0, -1.0],
                           ["NaN", "NaN"]]]}
    if index == 5:
        return {"rings": [[[0.0, 0.0], [0.0, 0.0], [1.0, 1.0], [1.0, 1.0]]]}
    return {"rings": [_cw_square(-70.0, 40.0, 0.5)]}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name, "type": esri_type, "nullable": nullable,
                 "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "geometryType": src.GEOMETRY_TYPE, "hasZ": False, "hasM": False,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": {"latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": ""}


def _fixture_sb_item() -> dict:
    return {"id": src.SB_ITEM_ID, "title": "PAD-US 4.1 Full Inventory Database",
            "citation": ("U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2024, "
                         "Protected Areas Database of the United States (PAD-US) 4.1: "
                         "U.S. Geological Survey data release, "
                         "https://doi.org/10.5066/P96WBCHS."),
            "files": [{"name": "PADUS4_1Geodatabase.zip",
                       "size": src.EXPECTED_ZIP_BYTES}]}


def _fixture_agol_item() -> dict:
    return {"id": src.AGOL_ITEM_ID, "owner": src.AGOL_ITEM_OWNER,
            "title": src.AGOL_ITEM_TITLE}


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        if isinstance(payload, bytes):
            self.body = payload
        else:
            self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict], *, layer_fields=None, count=None, ids=None,
                   service=None, layer=None, sb_item=None, agol_item=None,
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
    sb = sb_item if sb_item is not None else _fixture_sb_item()
    agol = agol_item if agol_item is not None else _fixture_agol_item()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}/query"):
            if query.get("returnCountOnly") == ["true"]:
                return httpx.Response(200, stream=JSONStream({"count": count}),
                                      headers={"content-type": "application/json"})
            if query.get("returnIdsOnly") == ["true"]:
                return httpx.Response(200, stream=JSONStream(
                    {"objectIdFieldName": "OBJECTID", "objectIds": objectids}),
                    headers={"content-type": "application/json"})
            if query.get("f") == ["pbf"]:
                offset = int(query["resultOffset"][0])
                record_count = int(query["resultRecordCount"][0])
                if record_count in refuse_counts:
                    return httpx.Response(200, stream=JSONStream(
                        {"error": {"code": 500, "message": "Error performing query operation",
                                   "details": []}}),
                        headers={"content-type": "application/json"})
                page = {"objectIdFieldName": "OBJECTID",
                        "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                             "latestWkid": src.SOURCE_CRS_WKID},
                        "hasZ": False, "hasM": False, "exceededTransferLimit": True,
                        "fields": [{"name": f["name"], "type": f["type"]}
                                   for f in layer_fields],
                        "features": rows[offset:offset + record_count]}
                return httpx.Response(200, stream=JSONStream(page),
                                      headers={"content-type": "application/octet-stream"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if str(request.url).startswith("https://www.arcgis.com/sharing/rest/content/items/"):
            return httpx.Response(200, stream=JSONStream(agol),
                                  headers={"content-type": "application/json"})
        if str(request.url).startswith("https://www.sciencebase.gov/catalog/item/"):
            return httpx.Response(200, stream=JSONStream(sb),
                                  headers={"content-type": "application/json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=2):
    import dagster_hifld.assets.pad_us_manager_name as asset_module

    def decode(body: bytes, *, allow_transfer_limit: bool = False):
        document = json.loads(body)
        if "error" in document:
            return document
        if not allow_transfer_limit and document.get("exceededTransferLimit"):
            raise ValueError("query_page_exceeded_transfer_limit")
        return document

    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_IDS_SHA256", ""),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_NAN_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", None),
        patch.object(src, "EXPECTED_OBSERVED_BOUNDS", None),
        patch.object(src, "SEGMENT_START_RECORDS", segment_size),
        patch.object(src, "SEGMENT_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(asset_module, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(src, "decode_feature_collection_pbf", decode),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(epa_frs_common, "validate_construction_code_identity"),
    ]
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


class PadUsManagerNameTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "pad-us-manager-name")
        self.assertEqual(src.SERVICE_URL,
                         "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services"
                         "/Manager_Name_PADUS/FeatureServer")
        self.assertEqual(src.LAYER_ID, 0)
        self.assertEqual(src.LAYER_NAME, "PADUS4_1ManagerName")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.EXPECTED_COUNT, 655_335)
        self.assertEqual(src.MAX_RECORD_COUNT, 2000)
        self.assertEqual(src.SEGMENT_START_RECORDS, 1000)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "700b505d62ec00dda9ca8bb23a44283f928be1167971bd409a6477b83a4e7510")
        self.assertEqual(src.EXPECTED_IDS_SHA256,
                         "c796d962b10cfb278175ca0076a528414fcd6241514a7f75dd6980c430b3153a")
        self.assertEqual(src.EXPECTED_ZIP_SHA256,
                         "62ed113335e359509ad68a731146c74aa873c5a92da4f3fda572e54bc9b823ab")
        self.assertEqual(src.EXPECTED_RECORD_SHA256, "")
        self.assertEqual(src.EXPECTED_GEOMETRY_SHA256, "")
        self.assertIsNone(src.EXPECTED_NULL_GEOMETRY_COUNT)
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_segment_query_contract_is_roster_bounded(self):
        url = src._segment_query(150000, 2000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["1=1"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultOffset"], ["150000"])
        self.assertEqual(query["resultRecordCount"], ["2000"])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["f"], ["pbf"])
        self.assertLess(len(url), 400)
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"], ["true"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"], ["true"])

    def test_polygon_wkb_keeps_ring_semantics(self):
        exterior = _cw_square(0.0, 0.0, 1.0)
        hole = _ccw_square(0.25, -0.5, 0.25)
        wkb = src._polygon_wkb({"rings": [exterior, hole]})
        order, kind = wkb[0], int.from_bytes(wkb[1:5], "little")
        self.assertEqual((order, kind), (1, 3))
        import struct
        (ring_count,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(ring_count, 2)
        multi = src._polygon_wkb({"rings": [_cw_square(0.0, 0.0, 1.0),
                                            _cw_square(2.0, 0.0, 1.0)]})
        self.assertEqual((multi[0], int.from_bytes(multi[1:5], "little")), (1, 6))

    def test_polygon_geometry_drift_is_refused(self):
        cases = {
            "unexpected_source_geometry_members": {"rings": [], "curves": []},
            "source_polygon_geometry_invalid": {"rings": []},
            "source_polygon_ordinate_contract_drift": {"rings": [[[0.0, 0.0, 0.0]]]},
            "source_polygon_ordinate_not_numeric": {"rings": [[["NaN", 0.0], [1.0, 0.0],
                                                               [1.0, -1.0], [0.0, -1.0],
                                                               ["NaN", 0.0]]]},
            "source_polygon_ordinate_not_finite": {"rings": [[[float("inf"), 0.0], [1.0, 0.0],
                                                              [1.0, -1.0], [0.0, -1.0],
                                                              [float("inf"), 0.0]]]},
            "source_coordinate_range_drift": {"rings": [[[200.0, 0.0], [1.0, 0.0],
                                                         [1.0, -1.0], [0.0, -1.0],
                                                         [200.0, 0.0]]]},
            "source_polygon_ring_degenerate": {"rings": [[[0.0, 0.0], [0.0, 0.0],
                                                          [1.0, 1.0], [1.0, 1.0]]]},
            "source_polygon_ring_self_intersecting": {"rings": [[[0.0, 0.0], [3.0, 4.0],
                                                                 [1.0, 4.0], [4.0, 0.0],
                                                                 [0.0, 0.0]]]},
            "source_polygon_hole_unassigned": {"rings": [_cw_square(0.0, 0.0, 1.0),
                                                         _ccw_square(5.0, 5.0, 1.0)]},
            "source_polygon_exterior_missing": {"rings": [_ccw_square(0.25, -0.75, 0.5)]},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.PadUsManagerNameError, expected_error):
                    src._polygon_wkb(geometry)

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_pad_us_manager_name(storage, version="fixture-run",
                                                        capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                second = src.capture_pad_us_manager_name(
                    storage, version="fixture-run-2", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(), FIXTURE_OBJECTIDS)
            nulls = table.column("geometry").to_pylist()
            self.assertIsNone(nulls[3])
            self.assertIsNone(nulls[4])
            self.assertIsNone(nulls[5])
            self.assertIsNotNone(nulls[0])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon", "MultiPolygon"])

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [pad_us_manager_name, pad_us_manager_name_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__pad_us_manager_name__pad_us_manager_name":
                                            {"config": {"capture_directory":
                                                        str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(checks["pad_us_manager_name_complete"].passed)
                    candidate = result.output_for_node(
                        "ingest__pad_us_manager_name__pad_us_manager_name")
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
                result = src.capture_pad_us_manager_name(
                    _staging(root), version="seal-run", capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(sealed["pagination"]["mode"],
                             "enumerated_roster_bisected_segments")
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["unusable_geometry_counts"],
                             {"source_polygon_ring_degenerate": 1})
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("services.arcgis.com", "www.arcgis.com",
                                         "www.sciencebase.gov") for host in hosts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_refused_pages_bisect_down_to_singles(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_pad_us_manager_name(
                    _staging(root), version="bisect-run", capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(sorted((entry["offset"], entry["count"])
                                        for entry in state["segments"]),
                                 [(offset, 1) for offset in range(FIXTURE_COUNT)])
                resumed = src.capture_pad_us_manager_name(
                    _staging(root), version="bisect-run-2", capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={1, 2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.PadUsManagerNameError,
                                            "source_single_feature_unservable"):
                    src.fetch_pad_us_manager_name_pages(root / "capture")
                self.assertTrue(sorted((root / "capture").glob("page_*.error-*.body")))

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "nullable": True, "length": 4,
                                            "domain": None}]),
            "source_schema_baseline_drift": dict(
                layer_fields=[{**field, "length": (field.get("length") or 0) + 1}
                              if field.get("length") else field
                              for field in _fixture_layer_fields()]),
            "layer_copyright_release_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "copyrightText": "Data refreshed October, 2026."}),
            "exact_padus_manager_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 1000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": {
                                              "latestWkid":
                                                  src.NATIVE_LAYER_SR["latestWkid"]}}}),
            "source_id_population_drift": dict(ids=FIXTURE_OBJECTIDS[:-1]),
            "sciencebase_corroboration_identity_drift": dict(
                sb_item={**_fixture_sb_item(), "citation": "different citation"}),
            "sciencebase_corroboration_zip_reference_drift": dict(
                sb_item={**_fixture_sb_item(), "files": [{"name": "other.zip",
                                                          "size": 5}]}),
            "agol_item_identity_drift": dict(
                agol_item={**_fixture_agol_item(), "owner": "someone_else"}),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"rings": [[v if v[0] != 0.0 else [200.0, v[1]]
                                                    for v in rows[0]["geometry"]["rings"][0]]]}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0], rows[0], *rows[1:]], count=FIXTURE_COUNT),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": rows[0]["attributes"]["OBJECTID"]}},
                *rows[1:]]),
            "source_native_type_changed:GIS_Acres": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "GIS_Acres": "123"}},
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
                        with self.assertRaisesRegex(src.PadUsManagerNameError, expected_error):
                            src.capture_pad_us_manager_name(
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
                src.capture_pad_us_manager_name(
                    _staging(root), version="gate-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.PadUsManagerNameError,
                                                "source_null_geometry_count_drift"):
                        src.fetch_pad_us_manager_name_pages(capture_root)
                with patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", {}):
                    with self.assertRaisesRegex(src.PadUsManagerNameError,
                                                "source_unusable_geometry_counts_drift"):
                        src.fetch_pad_us_manager_name_pages(capture_root)
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    with patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                                      baseline["source_geometry_sha256"]):
                        with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT",
                                          baseline["source_null_geometry_count"]):
                            with patch.object(src, "EXPECTED_NAN_GEOMETRY_COUNT",
                                              baseline["source_nan_geometry_count"]):
                                with patch.object(
                                        src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS",
                                        baseline["source_unusable_geometry_counts"]):
                                    again = src.capture_pad_us_manager_name(
                                        _staging(root), version="gate-run-verify",
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
                src.capture_pad_us_manager_name(
                    storage, version="pin-run", capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.PadUsManagerNameError,
                                                "source_id_roster_drift"):
                        src.capture_pad_us_manager_name(
                            storage, version="pin-run-drift",
                            capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertTrue(failure["reason_code"].startswith("source_id_roster_drift"))


if __name__ == "__main__":
    unittest.main()
