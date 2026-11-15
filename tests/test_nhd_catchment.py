"""Focused tests for the USGS 3DHP_all Catchment draft pipeline."""
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
from shapely import from_wkb

from dagster_hifld.assets.nhd_catchment import nhd_catchment, nhd_catchment_complete
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import nhd_catchment as src

FIXTURE_COUNT = 7
FIXTURE_OBJECTIDS = tuple(range(1, FIXTURE_COUNT + 1))


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:07X}",
        "featuredate": 1709563014000 + index,
        "mainstemid": f"0000{index}X" if index != 1 else None,
        "areasqkm": 0.0317 + index if index != 2 else None,
        "totestdrainareasqkm": 1.25 + index,
        "workunitid": "07060001",
        "Shape__Length": 6388.035227755926 + index,
        "Shape__Area": 461231.6270300725 + index,
    }


def _cw_square(x0: float, y0: float, size: float, z: float = 0.0) -> list:
    return [[x0, y0, z], [x0 + size, y0, z], [x0 + size, y0 - size, z],
            [x0, y0 - size, z], [x0, y0, z]]


def _ccw_square(x0: float, y0: float, size: float, z: float = 0.0) -> list:
    return [[x0, y0, z], [x0, y0 - size, z], [x0 + size, y0 - size, z],
            [x0 + size, y0, z], [x0, y0, z]]


def _fixture_geometry(oid: object) -> object:
    if oid == FIXTURE_OBJECTIDS[0]:
        rings = [_cw_square(0.0, 0.0, 1.0)]
    elif oid == FIXTURE_OBJECTIDS[1]:
        rings = [_cw_square(0.0, 0.0, 1.0), _ccw_square(0.25, -0.5, 0.25)]
    elif oid == FIXTURE_OBJECTIDS[2]:
        rings = [_cw_square(0.0, 0.0, 1.0), _cw_square(2.0, 0.0, 1.0)]
    elif oid == FIXTURE_OBJECTIDS[3]:
        rings = [_cw_square(0.0, -1.0, 3.0), _cw_square(0.5, -1.5, 1.0),
                 _ccw_square(0.75, -1.75, 0.5)]
    elif oid == FIXTURE_OBJECTIDS[4]:
        return None
    elif oid == FIXTURE_OBJECTIDS[5]:
        rings = [[["NaN", "NaN", "NaN"], [1.0, 0.0, 0.0], [1.0, -1.0, 0.0],
                  [0.0, -1.0, 0.0], ["NaN", "NaN", "NaN"]]]
    else:
        rings = [[[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [1.0, 1.0, 0.0], [1.0, 1.0, 0.0]]]
    return {"hasZ": True, "rings": rings}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name if name != "Shape__Length" else "shape_Length",
                 "type": esri_type, "nullable": nullable, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": False,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": {"wkid": src.NATIVE_LAYER_SR["wkid"],
                                       "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]},
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": {"latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


def _fixture_alias_service_doc() -> dict:
    doc = _fixture_service_doc()
    core = len(src.EXPECTED_SERVICE_LAYERS) - len(src.SPATIAL_METADATA_IDS)
    doc["layers"] = doc["layers"][:core] + [
        {"id": 90, "name": "Spatial Metadata - DEM LimitationArea"},
        {"id": 92, "name": "Spatial Metadata - EDH Workunit Area"},
        {"id": 94, "name": "Spatial Metadata - Elevation Workunit Area"},
    ]
    return doc


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
                   service=None, alias=None, layer=None, doc_page=None,
                   refuse_counts=(), page_has_m=None) -> httpx.MockTransport:
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
    alias_doc = alias if alias is not None else _fixture_alias_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            document = alias_doc if request.url.host == "hydro.nationalmap.gov" else service_doc
            return httpx.Response(200, stream=JSONStream(document),
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
        if query.get("resultOffset") is not None:
            offset = int(query["resultOffset"][0])
            record_count = int(query["resultRecordCount"][0])
            if record_count in refuse_counts:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query operation",
                               "details": []}}),
                    headers={"content-type": "application/json"})
            slice_ids = objectids[offset:offset + record_count]
            features = rows[offset:offset + record_count]
            page = {"objectIdFieldName": "OBJECTID",
                    "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                         "latestWkid": src.SOURCE_CRS_WKID},
                    "hasZ": True, "features": features,
                    "exceededTransferLimit": False}
            if page_has_m is not None:
                page["hasM"] = page_has_m
            return httpx.Response(200, stream=JSONStream(page),
                                  headers={"content-type": "application/json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=2):
    import dagster_hifld.assets.nhd_catchment as asset_module
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


class NhdCatchmentTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-catchment")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 80)
        self.assertEqual(src.LAYER_NAME, "Catchment")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.EXPECTED_COUNT, 300_070)
        self.assertEqual(src.MAX_RECORD_COUNT, 2500)
        self.assertEqual(src.SEGMENT_START_RECORDS, 2000)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "1b32b55650559d19c1d0fe2e3c876940d179925bf2d0f3bbe2d2ac262c7a57a5")
        self.assertEqual(src.EXPECTED_IDS_SHA256,
                         "1e2cb430b690820a3e2b5c8059094e5e95af54e4e426a8ffa6809e53345e33b8")
        self.assertEqual(src.EXPECTED_RECORD_SHA256,
                         "c2b619f60d52b84af3b07fc3eef6c97f0c905786c05f0469a81e4607cb187612")
        self.assertEqual(src.EXPECTED_GEOMETRY_SHA256,
                         "aa8ef29cd6a48212a5341d93b7641da92273bdd68ea29f941215db0b647de208")
        self.assertEqual(src.EXPECTED_NULL_GEOMETRY_COUNT, 0)
        self.assertEqual(src.EXPECTED_NAN_GEOMETRY_COUNT, 0)
        self.assertEqual(src.EXPECTED_EMPTY_GEOMETRY_COUNT, 0)
        self.assertEqual(src.EXPECTED_UNUSABLE_GEOMETRY_COUNTS, {})
        self.assertEqual(
            src.EXPECTED_OBSERVED_BOUNDS,
            {"xmin": -159.04621038268183, "ymin": 35.61857066855389,
             "xmax": -72.5853745558836, "ymax": 68.41390468924469})
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
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["returnM"], ["false"])
        self.assertLess(len(url), 400)
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"], ["true"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"], ["true"])

    def test_polygon_wkb_keeps_ring_semantics_and_z(self):
        import struct
        exterior = _cw_square(0.0, 0.0, 1.0)
        hole = _ccw_square(0.25, -0.5, 0.25)
        wkb = src._polygon_wkb({"hasZ": True, "rings": [exterior, hole]})
        assembled = from_wkb(wkb)
        self.assertEqual(assembled.geom_type, "Polygon")
        self.assertTrue(assembled.is_valid)
        order, kind = struct.unpack_from("<BI", wkb, 0)
        self.assertEqual((order, kind), (1, 0x80000003))
        (ring_count,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(ring_count, 2)
        offset = 9
        for ring in (exterior, hole):
            (points,) = struct.unpack_from("<I", wkb, offset)
            offset += 4
            self.assertEqual(points, len(ring))
            for vertex in ring:
                self.assertEqual(struct.unpack_from("<3d", wkb, offset), tuple(vertex))
                offset += 24
        self.assertEqual(offset, len(wkb))
        multi = src._polygon_wkb({"hasZ": True, "rings": [_cw_square(0.0, 0.0, 1.0),
                                                          _cw_square(2.0, 0.0, 1.0)]})
        self.assertEqual(struct.unpack_from("<BI", multi, 0), (1, 0x80000006))
        (parts,) = struct.unpack_from("<I", multi, 5)
        self.assertEqual(parts, 2)
        self.assertTrue(from_wkb(multi).is_valid)
        # A smaller exterior nested inside a larger exterior assembles an
        # OGC-invalid MultiPolygon ("Nested shells"): gated, never returned.
        with self.assertRaisesRegex(src.NhdCatchmentError, "source_polygon_assembly_invalid"):
            src._polygon_wkb({"hasZ": True, "rings": [_cw_square(0.0, -1.0, 3.0),
                                                      _cw_square(0.5, -1.5, 1.0),
                                                      _ccw_square(0.75, -1.75, 0.5)]})

    def test_polygon_geometry_drift_is_refused(self):
        base = {"hasZ": True, "rings": [_cw_square(0.0, 0.0, 1.0)]}
        cases = {
            "unexpected_source_geometry_members": {**base, "curves": []},
            "source_polygon_geometry_invalid": {"hasZ": True, "rings": []},
            "source_polygon_ordinate_contract_drift": {
                "hasZ": True, "rings": [[v[:2] for v in base["rings"][0]]]},
            "source_polygon_ordinate_not_numeric": {
                "hasZ": True, "rings": [[["NaN"] + v[1:] for v in base["rings"][0]]]},
            "source_polygon_ordinate_not_finite": {
                "hasZ": True, "rings": [[v[:2] + [float("inf")] for v in base["rings"][0]]]},
            "source_polygon_ring_degenerate": {
                "hasZ": True, "rings": [[[0.0, 0.0, 0.0], [0.0, 0.0, 0.0],
                                         [1.0, 1.0, 0.0], [1.0, 1.0, 0.0]]]},
            "source_polygon_ring_self_intersecting": {
                "hasZ": True, "rings": [[[0.0, 0.0, 0.0], [3.0, 4.0, 0.0],
                                         [1.0, 4.0, 0.0], [4.0, 0.0, 0.0],
                                         [0.0, 0.0, 0.0]]]},
            "source_polygon_hole_unassigned": {
                "hasZ": True, "rings": [_cw_square(0.0, 0.0, 1.0),
                                        _ccw_square(5.0, 5.0, 1.0)]},
            # A hole that pokes outside its only claimable exterior assembles
            # an OGC-invalid Polygon (Self-intersection where hole and shell
            # cross): gated through source_polygon_assembly_invalid.
            "source_polygon_assembly_invalid": {
                "hasZ": True, "rings": [_cw_square(0.0, 0.0, 2.0),
                                        _ccw_square(0.5, -0.5, 2.0)]},
            "source_polygon_exterior_missing": {
                "hasZ": True, "rings": [_ccw_square(0.25, -0.75, 0.5)]},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.NhdCatchmentError, expected_error):
                    src._polygon_wkb(geometry)

    def test_invalid_assembled_geometry_is_gated_counted_and_raw_rings_kept(self):
        rows = _fixture_rows()
        crossing_hole = {"hasZ": True, "rings": [_cw_square(0.0, 0.0, 2.0),
                                                 _ccw_square(0.5, -0.5, 2.0)]}
        for label, geometry in (("fixture nested shells", rows[3]["geometry"]),
                                ("crossing hole", crossing_hole)):
            with self.subTest(label):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "source_polygon_assembly_invalid"):
                    src._polygon_wkb(geometry)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                result = src.capture_nhd_catchment(storage, version="gate-invalid-run",
                                                   capture_directory=str(capture_root))
            self.assertEqual(result["source_unusable_geometry_counts"],
                             {"source_polygon_ring_degenerate": 1,
                              "source_polygon_assembly_invalid": 1})
            table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            records = table.to_pylist()
            gated = records[3]
            self.assertEqual(gated["OBJECTID"], 4)
            self.assertIsNone(gated["geometry"])
            self.assertEqual({key: value for key, value in gated.items()
                              if key != "geometry"}, _fixture_attrs(4))
            for index in (0, 1, 2):
                self.assertIsNotNone(records[index]["geometry"])
                self.assertTrue(from_wkb(records[index]["geometry"]).is_valid)
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["unusable_geometry_counts"],
                             {"source_polygon_ring_degenerate": 1,
                              "source_polygon_assembly_invalid": 1})
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_nhd_catchment(storage, version="fixture-run",
                                                  capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_catchment(storage, version="fixture-run-2",
                                                   capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(), list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("id3dhp").to_pylist(),
                             [row["attributes"]["id3dhp"] for row in rows])
            nulls = table.column("geometry").to_pylist()
            self.assertIsNone(nulls[3])
            self.assertIsNone(nulls[4])
            self.assertIsNone(nulls[5])
            self.assertIsNone(nulls[6])
            for index in (0, 1, 2):
                self.assertIsNotNone(nulls[index])
                self.assertTrue(from_wkb(nulls[index]).is_valid)
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon Z", "MultiPolygon Z"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""),
                             "WGS 84")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_catchment, nhd_catchment_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__nhd_catchment__nhd_catchment":
                                            {"config": {"capture_directory":
                                                        str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(checks["nhd_catchment_complete"].passed)
                    candidate = result.output_for_node("ingest__nhd_catchment__nhd_catchment")
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
                result = src.capture_nhd_catchment(_staging(root), version="seal-run",
                                                   capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(sealed["pagination"]["mode"],
                             "enumerated_roster_bisected_segments")
            self.assertEqual(sealed["pagination"]["segment_start_records"], 2)
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["null_geometry_count"], 1)
            self.assertEqual(sealed["nan_geometry_count"], 1)
            self.assertEqual(sealed["unusable_geometry_counts"],
                             {"source_polygon_ring_degenerate": 1,
                              "source_polygon_assembly_invalid": 1})
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov", "hydro.nationalmap.gov",
                                         "www.usgs.gov") for host in hosts))
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
                result = src.capture_nhd_catchment(_staging(root), version="bisect-run",
                                                   capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(sorted((entry["offset"], entry["count"])
                                        for entry in state["segments"]),
                                 [(offset, 1) for offset in range(FIXTURE_COUNT)])
                resumed = src.capture_nhd_catchment(_staging(root), version="bisect-run-2",
                                                    capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={1, 2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "source_single_feature_unservable"):
                    src.fetch_nhd_catchment_pages(root / "capture")
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
            "usgs_3dhp_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "usgs_3dhp_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "usgs_3dhp_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy NHD service."}),
            "exact_3dhp_catchment_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_page_z_or_m_flag_missing": dict(page_has_m=True),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": {
                                              "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}}}),
            "source_id_population_drift": dict(ids=list(FIXTURE_OBJECTIDS)[:-1]),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_polygon_ordinate_contract_drift": dict(rows=[
                {**rows[0], "geometry": {"hasZ": True,
                                         "rings": [[v[:2] for v in
                                                    rows[0]["geometry"]["rings"][0]]]}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"hasZ": True, "rings": [
                    [[200.0, v[1], v[2]] if v[0] == 0.0 else v
                     for v in rows[0]["geometry"]["rings"][0]]]}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0], rows[0], *rows[1:]], count=FIXTURE_COUNT),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "id3dhp": "0000001"}},
                *rows[1:]]),
            "source_native_type_changed:areasqkm": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "areasqkm": "0.03"}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
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
                        with self.assertRaisesRegex(src.NhdCatchmentError, expected_error):
                            src.capture_nhd_catchment(storage, version="drift-run",
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
                src.capture_nhd_catchment(_staging(root), version="gate-run",
                                          capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.NhdCatchmentError,
                                                "source_null_geometry_count_drift"):
                        src.fetch_nhd_catchment_pages(capture_root)
                with patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", {}):
                    with self.assertRaisesRegex(src.NhdCatchmentError,
                                                "source_unusable_geometry_counts_drift"):
                        src.fetch_nhd_catchment_pages(capture_root)
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    with patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                                      baseline["source_geometry_sha256"]):
                        with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT",
                                          baseline["source_null_geometry_count"]):
                            with patch.object(src, "EXPECTED_NAN_GEOMETRY_COUNT",
                                              baseline["source_nan_geometry_count"]):
                                with patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS",
                                                  baseline["source_unusable_geometry_counts"]):
                                    again = src.capture_nhd_catchment(
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
                src.capture_nhd_catchment(storage, version="pin-run",
                                          capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.NhdCatchmentError,
                                                "source_id_roster_drift"):
                        src.capture_nhd_catchment(storage, version="pin-run-drift",
                                                  capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertTrue(failure["reason_code"].startswith("source_id_roster_drift"))


if __name__ == "__main__":
    unittest.main()
