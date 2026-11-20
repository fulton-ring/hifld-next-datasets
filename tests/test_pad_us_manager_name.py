"""Focused tests for the USGS PAD-US Manager Name draft pipeline."""
import io
import gzip
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
    pad_us_manager_name_approved_baseline_drift,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import arcgis_layer as shared
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.esri_pbf import decode_feature_collection_pbf
import shapely

from dagster_hifld.sources import pad_us_manager_name as src
from dagster_hifld.sources.esri_geometry import to_wkb

FIXTURE_COUNT = 7
FIXTURE_OBJECTIDS = [2, 3, 7, 12, 41, 58, 900]  # sparse, non-contiguous roster (ascending)
FIXTURE_LAYER_EXTENT = {"xmin": -20037507.0672, "ymin": -1733741.441300001,
                        "xmax": 20037507.067199998, "ymax": 12930675.6069}


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
        # A closed zero-area double-back ring: the producer's degenerate-ring
        # shape, retained OGC-invalid through the shared converter.
        return {"rings": [[[0.0, 0.0], [0.0, 0.0], [1.0, 1.0], [1.0, 1.0], [0.0, 0.0]]]}
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
            "extent": {**FIXTURE_LAYER_EXTENT,
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
                        "hasZ": False, "hasM": False,
                        "exceededTransferLimit": offset + record_count < len(objectids),
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
    def decode(body: bytes, *, allow_transfer_limit: bool = False):
        document = json.loads(body)
        if "error" in document:
            return document
        if not allow_transfer_limit and document.get("exceededTransferLimit"):
            raise ValueError("query_page_exceeded_transfer_limit")
        return document

    contexts = [
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_NAN_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_EMPTY_GEOMETRY_COUNT", None),
        patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", None),
        patch.object(src, "SEGMENT_START_RECORDS", segment_size),
        patch.object(src, "SEGMENT_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "decode_feature_collection_pbf", decode),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(capture_common, "validate_construction_code_identity"),
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
    def test_saved_native_pbf_capture_replay_and_refresh_preserve_originals(self):
        body = (Path(__file__).parent / "fixtures"
                / "pad_us_manager_name_oid3651_null_geometry.pbf").read_bytes()
        decoded = decode_feature_collection_pbf(body, allow_transfer_limit=True)
        features = decoded["features"]
        base = make_responder(features, ids=[3651])
        wire_mtime = {"value": 0}

        def respond(request):
            query = parse_qs(request.url.query.decode())
            if query.get("f") == ["pbf"]:
                self.assertEqual(query["resultOffset"], ["0"])
                self.assertEqual(query["resultRecordCount"], ["1"])
                return httpx.Response(200, headers={"Content-Encoding": "gzip"},
                    stream=httpx.ByteStream(gzip.compress(body, mtime=wire_mtime["value"])))
            return base.handler(request)

        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = httpx.MockTransport(respond)
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
            patch.object(src, "SEGMENT_START_RECORDS", 1),
            patch.object(src, "_default_client",
                         side_effect=lambda: httpx.Client(transport=transport)),
            patch.object(src, "_doc_client",
                         side_effect=lambda: httpx.Client(transport=transport)),
        ):
            root = Path(tmp)
            storage = _staging(root)
            first = src.capture_pad_us_manager_name(storage, version="native",
                                                   capture_directory=str(root / "capture"))
            self.assertEqual(first["feature_count"], 1)
            self.assertEqual(first["source_null_geometry_count"], 1)
            prefix = first["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
            original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
            table = pq.read_table(root / "capture" / f"{src.DATASET_SLUG}.parquet")
            self.assertEqual(table.drop(["geometry"]).to_pylist(), [features[0]["attributes"]])
            self.assertEqual(table.column("OBJECTID").to_pylist(), [3651])
            self.assertEqual(table.column("Mang_Name").to_pylist(), ["BLM"])
            self.assertEqual(table.column("State_Nm").to_pylist(), ["WY"])
            self.assertEqual(table.column("geometry").to_pylist(), [None])
            self.assertEqual(table.column("EsmtHldr").to_pylist(), [None])
            crs = json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
            self.assertEqual(crs["id"], {"authority": "EPSG", "code": 4326})
            self.assertEqual(storage.read_key(prefix + "/raw/page_000000_00001.body"), body)
            with patch.object(httpx.Client, "send",
                              side_effect=AssertionError("unexpected HTTP during complete replay")):
                replay = src.capture_pad_us_manager_name(storage, version="warm",
                                                        capture_directory=str(root / "capture"))
                self.assertTrue(replay["reused"])
                cold = src.capture_pad_us_manager_name(
                    StagingStorageResource(local_dir=str(root / "cold-stage")), version="cold",
                    capture_directory=str(root / "capture"))
                self.assertEqual(cold["version"], first["version"])
            wire_mtime["value"] = 1
            refreshed = src.capture_pad_us_manager_name(storage, version="refresh",
                                                       capture_directory=str(root / "fresh"))
            self.assertTrue(refreshed["reused"])
            self.assertNotEqual((root / "fresh" / "page_000000_00001.wire").read_bytes(),
                                (root / "capture" / "page_000000_00001.wire").read_bytes())
            for result in (replay, cold, refreshed):
                for key in ("feature_count", "source_reported_count", "source_population_complete",
                            "source_null_geometry_count", "source_id_sha256", "source_schema_sha256"):
                    self.assertEqual(result[key], first[key])
            self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix(prefix)},
                             original)

    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "pad-us-manager-name")
        self.assertEqual(src.SERVICE_URL,
                         "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services"
                         "/Manager_Name_PADUS/FeatureServer")
        self.assertEqual(src.LAYER_ID, 0)
        self.assertEqual(src.LAYER_NAME, "PADUS4_1ManagerName")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.MAX_RECORD_COUNT, 2000)
        self.assertEqual(src.SEGMENT_START_RECORDS, 1000)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "700b505d62ec00dda9ca8bb23a44283f928be1167971bd409a6477b83a4e7510")
        self.assertEqual(src.EXPECTED_ZIP_SHA256,
                         "62ed113335e359509ad68a731146c74aa873c5a92da4f3fda572e54bc9b823ab")
        self.assertFalse(hasattr(src, "EXPECTED_COUNT"))
        self.assertFalse(hasattr(src, "EXPECTED_IDS_SHA256"))
        self.assertFalse(hasattr(src, "EXPECTED_RECORD_SHA256"))
        self.assertFalse(hasattr(src, "EXPECTED_GEOMETRY_SHA256"))
        self.assertFalse(hasattr(src, "EXPECTED_NULL_GEOMETRY_COUNT"))
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
        def converted(rings):
            return to_wkb({"rings": rings}, src.GEOMETRY_TYPE, has_z=False, has_m=False)

        exterior = _cw_square(0.0, 0.0, 1.0)
        hole = _ccw_square(0.25, -0.5, 0.25)
        wkb = converted([exterior, hole])
        order, kind = wkb[0], int.from_bytes(wkb[1:5], "little")
        self.assertEqual((order, kind), (1, 3))
        import struct
        (ring_count,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(ring_count, 2)
        multi = converted([_cw_square(0.0, 0.0, 1.0), _cw_square(2.0, 0.0, 1.0)])
        self.assertEqual((multi[0], int.from_bytes(multi[1:5], "little")), (1, 6))

    def test_polygon_geometry_drift_is_refused(self):
        cases = {
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
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.PadUsManagerNameError, expected_error):
                    src._validate_polygon_geometry(geometry["rings"])
        with self.assertRaisesRegex(src.PadUsManagerNameError, "unexpected_source_geometry_members"):
            src._validate_feature({"attributes": _fixture_attrs(1),
                                   "geometry": {"rings": [], "curves": []}},
                                  tuple(name for name, *_rest in src.NATIVE_FIELDS), 1)
        # GDAL retains a self-intersecting exterior, an unassigned hole and an
        # all-CCW ring set verbatim; only the degenerate double-back ring
        # becomes OGC-invalid (issue #997 shared converter).
        def converted(rings):
            return to_wkb({"rings": rings}, src.GEOMETRY_TYPE, has_z=False, has_m=False)

        self.assertFalse(shapely.from_wkb(converted(
            [[[0.0, 0.0], [3.0, 4.0], [1.0, 4.0], [4.0, 0.0], [0.0, 0.0]]])).is_valid)
        self.assertTrue(shapely.from_wkb(converted(
            [_cw_square(0.0, 0.0, 1.0), _ccw_square(5.0, 5.0, 1.0)])).is_valid)
        self.assertTrue(shapely.from_wkb(converted(
            [_ccw_square(0.25, -0.75, 0.5)])).is_valid)
        self.assertFalse(shapely.from_wkb(converted(
            [[[0.0, 0.0], [0.0, 0.0], [1.0, 1.0], [1.0, 1.0], [0.0, 0.0]]])).is_valid)

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
            self.assertIsNotNone(nulls[5])
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
                        [pad_us_manager_name, pad_us_manager_name_complete,
                         pad_us_manager_name_approved_baseline_drift],
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
                    warning = checks["pad_us_manager_name_approved_baseline_drift"]
                    self.assertFalse(warning.passed)
                    self.assertEqual(warning.severity.value, "WARN")
                    self.assertEqual(warning.metadata["comparison_status"].value,
                                     "skipped_no_approved_baseline")
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
            self.assertEqual(sealed["unusable_geometry_counts"], {})
            self.assertEqual(sealed["ogc_invalid_geometry_sample_objectids"],
                             [FIXTURE_OBJECTIDS[5]])
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

    def test_tampered_staged_page_evidence_refuses_reuse(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_pad_us_manager_name(
                    storage, version="tamper-run", capture_directory=str(capture_root))
            self.assertFalse(first["reused"])
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{first['version']}"
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            page = str(state["segments"][0]["name"])
            body_path = root / "staging" / prefix / "raw" / f"{page}.body"
            body_path.write_bytes(body_path.read_bytes() + b"trailing-tamper")
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        ValueError, "cached_candidate_object_integrity_failed"):
                    src.capture_pad_us_manager_name(
                        _staging(root), version="tamper-run-2",
                        capture_directory=str(capture_root))

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
            "source_count_or_id_roster_drift": dict(count=FIXTURE_COUNT + 1),
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
            "source_layer_extent_spatial_reference_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**FIXTURE_LAYER_EXTENT,
                                          "spatialReference": {
                                              "latestWkid": 4326}}}),
            "source_id_population_drift": dict(ids=[*FIXTURE_OBJECTIDS[:-1], 2]),
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
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        for name, value, error in (
            ("EXPECTED_NAN_GEOMETRY_COUNT", 0, "source_nan_geometry_count_drift"),
            ("EXPECTED_EMPTY_GEOMETRY_COUNT", 0, "source_empty_geometry_count_drift"),
            ("EXPECTED_UNUSABLE_GEOMETRY_COUNTS", {"conversion_failure": 1},
             "source_unusable_geometry_counts_drift"),
        ):
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                rows = _fixture_rows()
                if name == "EXPECTED_EMPTY_GEOMETRY_COUNT":
                    rows[4] = {**rows[4], "geometry": {"rings": []}}
                with fixture_mode(schema_sha, make_responder(rows)):
                    with patch.object(src, name, value):
                        with self.assertRaisesRegex(src.PadUsManagerNameError, error):
                            src.capture_pad_us_manager_name(_staging(root), version="gate",
                                capture_directory=str(root / "capture"))
                self.assertFalse(src._load_state(root / "capture")["complete"])
                self.assertTrue(list((root / "staging" / "_quarantine").rglob("failure.json")))

    def test_changed_populations_complete_but_warn_against_approved_release(self):
        from dagster import AssetCheckSeverity
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        populations = [
            rows,
            rows[:-1],
            [*rows, {"attributes": _fixture_attrs(1001), "geometry": rows[0]["geometry"]}],
            [{**rows[0], "attributes": {**rows[0]["attributes"], "Mang_Name": "Changed"},
              "geometry": {"rings": [_cw_square(8.0, 8.0, 1.0)]}}, *rows[1:]],
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidates = []
            for index, population in enumerate(populations):
                ids = [row["attributes"]["OBJECTID"] for row in population]
                transport = make_responder(population, ids=ids)
                with fixture_mode(schema_sha, transport):
                    candidate = src.capture_pad_us_manager_name(_staging(root),
                        version=f"population-{index}", capture_directory=str(root / str(index)))
                self.assertEqual(candidate["feature_count"], len(ids))
                self.assertEqual(candidate["source_reported_count"], len(ids))
                self.assertTrue(candidate["source_population_complete"])
                self.assertFalse(candidate["release_accepted"])
                self.assertEqual(candidate["source_id_sha256"], src._sha(src._canonical(ids)))
                candidates.append(candidate)
            self.assertEqual(len({item["version"] for item in candidates}), 4)
            baseline = {**candidates[0], "release_accepted": True}
            for candidate in candidates[1:3]:
                warning = approved_baseline_drift(candidate, baseline)
                self.assertFalse(warning.passed)
                self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
                self.assertTrue(warning.metadata["native_id_roster_changed"].value)
            skipped = approved_baseline_drift(candidates[0])
            self.assertEqual(skipped.metadata["comparison_status"].value,
                             "skipped_no_approved_baseline")

    def test_retained_transport_is_bounded_request_bound_and_wire_verified(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = ("missing_wire", "malformed_gzip", "wrong_decoded_body",
                 "wrong_request", "oversized")
        for case in cases:
            with self.subTest(case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                    src.fetch_pad_us_manager_name_pages(root)
                    name = src._load_state(root)["segments"][0]["name"]
                    body, receipt = src._read_capture(root, name)
                    self.assertEqual(shared.load_replay(root, name)[0], body)
                    wire_path = root / f"{name}.wire"
                    if case == "missing_wire":
                        wire_path.unlink()
                    elif case in ("malformed_gzip", "wrong_decoded_body"):
                        wire = (b"not gzip" if case == "malformed_gzip"
                                else gzip.compress(b"different decoded body", mtime=0))
                        wire_path.write_bytes(wire)
                        receipt.update(wire_bytes=len(wire), wire_sha256=src._sha(wire),
                                       content_encoding="gzip", content_length=len(wire))
                    elif case == "wrong_request":
                        receipt["url"] = receipt["final_url"] = src._segment_query(2, 2)
                    (root / f"{name}.receipt.json").write_bytes(src._canonical(receipt))
                    with patch.object(src, "MAX_BODY_BYTES",
                                      len(body) - 1 if case == "oversized" else src.MAX_BODY_BYTES):
                        with self.assertRaisesRegex(src.PadUsManagerNameError,
                                                    "retained_capture_receipt_invalid"):
                            src._read_capture(root, name)

    def test_resume_refetches_entire_closing_group_after_another_interruption(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(_fixture_rows())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                src.fetch_pad_us_manager_name_pages(root)
                name = src._load_state(root)["segments"][0]["name"]
                (root / f"{name}.wire").unlink()
                original = src._capture_with_retry
                calls = []

                def interrupted(client, url, capture_root, capture_name, **kwargs):
                    calls.append(capture_name)
                    self.assertFalse(src._load_state(root)["complete"])
                    if capture_name == "count_after":
                        raise ValueError("interrupted_closing")
                    return original(client, url, capture_root, capture_name, **kwargs)

                with patch.object(src, "_capture_with_retry", side_effect=interrupted):
                    with self.assertRaisesRegex(src.PadUsManagerNameError, "source_capture_failed"):
                        src.fetch_pad_us_manager_name_pages(root)
                self.assertFalse(src._load_state(root)["complete"])
                self.assertTrue(list(root.glob("layer_after.error-*.body")))
                self.assertIn(name, calls)
                calls.clear()

                def observed(client, url, capture_root, capture_name, **kwargs):
                    calls.append(capture_name)
                    self.assertFalse(src._load_state(root)["complete"])
                    return original(client, url, capture_root, capture_name, **kwargs)

                with patch.object(src, "_capture_with_retry", side_effect=observed):
                    src.fetch_pad_us_manager_name_pages(root)
                self.assertEqual(calls, list(src.AFTER_NAMES))
                self.assertTrue(src._load_state(root)["complete"])
                with patch.object(httpx.Client, "send", side_effect=AssertionError("unexpected HTTP")):
                    src.fetch_pad_us_manager_name_pages(root)

    def test_resumed_closing_count_or_roster_drift_refuses_and_quarantines(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        for drift in ("count", "ids", "metadata"):
            with self.subTest(drift), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture = root / "capture"
                with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                    src.fetch_pad_us_manager_name_pages(capture)
                state = src._load_state(capture)
                state["complete"] = False
                src._save_state(capture, state)
                kwargs = ({"count": FIXTURE_COUNT + 1} if drift == "count" else
                          {"ids": [*FIXTURE_OBJECTIDS[:-1], 901]} if drift == "ids" else
                          {"layer": {**_fixture_layer_doc(_fixture_layer_fields()),
                                     "editingInfo": {"lastEditDate": 123}}})
                with fixture_mode(schema_sha, make_responder(_fixture_rows(), **kwargs)):
                    with self.assertRaises(src.PadUsManagerNameError):
                        src.capture_pad_us_manager_name(_staging(root), version="drift",
                                                       capture_directory=str(capture))
                self.assertFalse(src._load_state(capture)["complete"])
                for name in src.AFTER_NAMES:
                    self.assertTrue(list(capture.glob(f"{name}.error-*.body")))
                self.assertTrue(list((root / "staging" / "_quarantine").rglob("failure.json")))

    def test_resume_replaces_corrupt_parent_checkpoint_after_bisection(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                src.fetch_pad_us_manager_name_pages(root)
                name = src._load_state(root)["segments"][0]["name"]
                (root / f"{name}.body").write_bytes(b"corrupt")
            with fixture_mode(schema_sha, make_responder(_fixture_rows(), refuse_counts={2})):
                src.fetch_pad_us_manager_name_pages(root)
                state = src._load_state(root)
                self.assertTrue(state["complete"])
                self.assertNotIn(name, [entry["name"] for entry in state["segments"]])
                self.assertEqual(sum(entry["count"] for entry in state["segments"]), FIXTURE_COUNT)
                src._stream_validate(root, parquet_sink=None)

    def test_torn_opening_metadata_and_missing_segment_body_preserve_witnesses(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                src.fetch_pad_us_manager_name_pages(root)
                name = src._load_state(root)["segments"][0]["name"]
                receipt = (root / f"{name}.receipt.json").read_bytes()
                (root / f"{name}.body").unlink()
                opening = (root / "layer_before.body").read_bytes()
                (root / "layer_before.wire").unlink()
                src.fetch_pad_us_manager_name_pages(root)
                self.assertTrue(src._load_state(root)["complete"])
                self.assertIn(receipt, [path.read_bytes()
                                       for path in root.glob(f"{name}.error-*.receipt.json")])
                self.assertIn(opening, [path.read_bytes()
                                       for path in root.glob("layer_before.error-*.body")])

    def test_capture_budget_refuses_partial_candidate_without_releasing_it(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = root / "capture"
            with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                src.fetch_pad_us_manager_name_pages(capture)
                name = src._load_state(capture)["segments"][0]["name"]
                (capture / f"{name}.body").unlink()
                with patch.object(src, "MAX_CAPTURE_BYTES", 0):
                    with self.assertRaises(src.PadUsManagerNameError):
                        src.capture_pad_us_manager_name(_staging(root), version="budget",
                                                       capture_directory=str(capture))
                self.assertFalse(src._load_state(capture)["complete"])
                self.assertFalse(list((root / "staging").rglob("candidate_ready.json")))
                self.assertTrue(list((root / "staging" / "_quarantine").rglob("failure.json")))


if __name__ == "__main__":
    unittest.main()
