"""Focused tests for the USGS 3DHP_all Flowline draft pipeline."""
import io
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.nhd_flowline import nhd_flowline, nhd_flowline_complete
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import nhd_flowline as src

FIXTURE_COUNT = 5
FIXTURE_OBJECTIDS = (1, 2, 3, 5, 8)


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:07X}",
        "featuredate": 1695340800000 + index,
        "mainstemid": f"https://geoconnex.us/usgs/mainstems/{oid}" if index != 1 else None,
        "gnisid": 694899 + index if index != 2 else None,
        "gnisidlabel": "Fixture River" if index != 2 else None,
        "featuretype": [1, 5, 3, 2, 7][index],
        "featuretypelabel": ["Channel Line", "Waterbody Connector", "Drainageway", "Canal",
                             "Hydro Unenforced Connector"][index],
        "lengthkm": 0.24430194 + index,
        "waterbodyid3dhp": None,
        "flowdirection": 1 if index != 3 else 0,
        "flowdirectionlabel": "Flow direction is in digitized direction, and z-values on "
                              "vertices flow downslope",
        "catchmentid3dhp": "",
        "flowpathid3dhp": None,
        "streamlevel": 3 + index,
        "startflag": index % 2,
        "terminalflag": 0,
        "streamorder": 1 + index,
        "streamcalculator": 1,
        "hydrosequence": 15221380.0 + index,
        "dnhydrosequence": 15215387.0 if index else 0.0,
        "uphydrosequence": 15227361.0 if index else 0.0,
        "dnlevelpath": 14762549.0 + index,
        "uplevelpath": 15221380.0 + index,
        "pathlength": 424.97036064 + index,
        "arbolatesum": 3.92010757 + index,
        "divergence": 0,
        "divergencelabel": "No divergence",
        "rtrndivergence": 0,
        "levelpath": 15221380 + index,
        "terminalpath": 25.0 + index,
        "workunitid": "NHD",
        "Shape__Length": 61.22714663080581 + index,
    }


def _fixture_geometry(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    vertex = lambda offset, measure: [
        -89.96433399428682 + index * 0.01 + offset * 0.001,
        31.89971254081809 + index * 0.01 + offset * 0.001,
        5.754000000000815 - offset,
        measure + index,
    ]
    if oid == FIXTURE_OBJECTIDS[2]:
        paths = [[vertex(0, 17.605999999999767), vertex(1, 9.0), vertex(2, 0.0)],
                 [vertex(3, 100.0), vertex(4, 0.0)]]
    else:
        paths = [[vertex(0, 17.605999999999767), vertex(1, 4.0), vertex(2, 0.0)]]
    return {"hasZ": True, "hasM": True, "paths": paths}


def _fixture_rows(count: int = FIXTURE_COUNT) -> list[dict]:
    return [{"attributes": _fixture_attrs(oid),
             "geometry": _fixture_geometry(oid)} for oid in FIXTURE_OBJECTIDS[:count]]


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
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": True,
            "maxRecordCount": src.PAGE_SIZE,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": dict(src.NATIVE_LAYER_SR),
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": dict(src.NATIVE_LAYER_SR)},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


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


def make_responder(rows: list[dict], *, layer_fields=None, count=None, service=None,
                   layer=None, geometry=None, doc_page=None) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
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
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if query.get("where") == ["1=1"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        last = int(query["where"][0].split(">")[1])
        page_rows = [row for row in rows
                     if (isinstance(row["attributes"]["OBJECTID"], int)
                         and row["attributes"]["OBJECTID"] > last)
                     or not isinstance(row["attributes"]["OBJECTID"], int)][:src.PAGE_SIZE]
        if geometry is not None:
            page_rows = [dict(row, geometry=geometry(row)) for row in page_rows]
        more = False
        if page_rows:
            page_oids = [row["attributes"]["OBJECTID"] for row in page_rows
                         if isinstance(row["attributes"]["OBJECTID"], int)]
            tail_oids = [row["attributes"]["OBJECTID"] for row in rows
                         if isinstance(row["attributes"]["OBJECTID"], int)]
            more = bool(page_oids) and max(page_oids) < max(tail_oids)
        return httpx.Response(200, stream=JSONStream(
            {"objectIdFieldName": "OBJECTID",
             "spatialReference": {"wkid": src.SOURCE_CRS_WKID, "latestWkid": src.SOURCE_CRS_WKID},
             "hasZ": True, "hasM": True, "features": page_rows,
             "exceededTransferLimit": more}),
            headers={"content-type": "application/json"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, page_size=None,
                 real_identity: bool = False):
    import dagster_hifld.assets.nhd_flowline as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_IDS_SHA256", ""),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        # CI runners hold far less than the pinned capture floor, so the floor
        # gate is evaluated against a stub that reports exactly the pinned
        # requirement; the refusal itself is exercised by the dedicated floor
        # test below.
        patch.object(src.shutil, "disk_usage",
                     return_value=SimpleNamespace(free=src.MIN_FREE_BYTES)),
    ]
    if not real_identity:
        contexts.extend([
            patch.object(src, "_code_identity",
                         return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                       "files": [], "working_tree_status": "unknown"}),
            patch.object(src, "validate_construction_code_identity"),
            patch.object(epa_frs_common, "validate_construction_code_identity"),
        ])
    if page_size is not None:
        contexts.append(patch.object(src, "PAGE_SIZE", page_size))
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


class NhdFlowlineTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-flowline")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 50)
        self.assertEqual(src.LAYER_NAME, "Flowline")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolyline")
        self.assertEqual(src.EXPECTED_COUNT, 30_251_533)
        self.assertEqual(src.PAGE_SIZE, 2500)
        self.assertEqual(len(src.NATIVE_FIELDS), 33)
        self.assertEqual(src.MIN_FREE_BYTES, 350_000_000_000)
        self.assertEqual(src.EXPECTED_RECORD_SHA256, "")
        self.assertEqual(src.EXPECTED_GEOMETRY_SHA256, "")
        self.assertEqual(src.EXPECTED_IDS_SHA256, "")
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_keyset_pagination_contract_is_ordered_and_bounded(self):
        url = src._page_url(15000000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["OBJECTID>15000000"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultRecordCount"], [str(src.PAGE_SIZE)])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["returnM"], ["true"])
        self.assertLess(len(url), 400)

    def test_polyline_wkb_keeps_zm_and_path_order(self):
        geometry = _fixture_geometry(FIXTURE_OBJECTIDS[2])
        wkb = src._polyline_wkb(geometry)
        order, kind = struct.unpack_from("<BI", wkb, 0)
        self.assertEqual((order, kind), (1, 0xC0000005))
        (parts,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(parts, 2)
        offset = 9
        for part_index, path in enumerate(geometry["paths"]):
            part_order, part_kind = struct.unpack_from("<BI", wkb, offset)
            offset += 5
            (count,) = struct.unpack_from("<I", wkb, offset)
            offset += 4
            self.assertEqual((part_order, part_kind, count), (1, 0xC0000002, len(path)))
            for vertex in path:
                self.assertEqual(struct.unpack_from("<4d", wkb, offset), tuple(vertex))
                offset += 32
        self.assertEqual(offset, len(wkb))
        single = src._polyline_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[0]))
        self.assertEqual(struct.unpack_from("<BI", single, 0), (1, 0xC0000002))

    def test_polyline_wkb_round_trips_through_independent_readers(self):
        """WKB counts are uint32; independent decoders must read every ordinate.

        Writing vertex or part counts as uint64 makes every independent reader
        (shapely/GEOS, GDAL) decode garbage coordinates or the wrong geometry,
        even when a byte-layout test repeats the same wrong layout.
        """
        import shapely

        def independent_decode(wkb: bytes) -> list[list[tuple[float, float, float, float]]]:
            order, kind, count = struct.unpack_from("<BII", wkb, 0)
            self.assertEqual(order, 1)
            self.assertIn(kind, (0xC0000002, 0xC0000005))
            offset = 9
            if kind == 0xC0000005:
                parts = []
                for _part in range(count):
                    part_order, part_kind, vertices = struct.unpack_from("<BII", wkb, offset)
                    offset += 9
                    self.assertEqual((part_order, part_kind), (1, 0xC0000002))
                    parts.append([struct.unpack_from("<4d", wkb, offset + 32 * index)
                                  for index in range(vertices)])
                    offset += 32 * vertices
                return parts
            return [[struct.unpack_from("<4d", wkb, offset + 32 * index)
                     for index in range(count)]]

        expected_by_oid = {}
        for oid in FIXTURE_OBJECTIDS:
            geometry = _fixture_geometry(oid)
            expected = [[tuple(vertex) for vertex in path] for path in geometry["paths"]]
            expected_by_oid[oid] = expected
            decoded = independent_decode(src._polyline_wkb(geometry))
            self.assertEqual(decoded, expected)
            shapely_geometry = shapely.from_wkb(src._polyline_wkb(geometry))
            kind = shapely.LineString if len(expected) == 1 else shapely.MultiLineString
            self.assertIsInstance(shapely_geometry, kind)
            if len(expected) > 1:
                self.assertEqual(shapely.get_num_geometries(shapely_geometry), len(expected))
            self.assertEqual(shapely.get_coordinates(shapely_geometry, include_z=True,
                                                     include_m=True).tolist(),
                             [list(vertex) for path in expected for vertex in path])
        minimal = {"hasZ": True, "hasM": True,
                   "paths": [[[1, 2, 3, 4], [5, 6, 7, 8]],
                             [[9, 10, 11, 12], [13, 14, 15, 16]]]}
        decoded = shapely.from_wkb(src._polyline_wkb(minimal))
        self.assertIsInstance(decoded, shapely.MultiLineString)
        self.assertEqual(shapely.get_num_geometries(decoded), 2)
        self.assertEqual(shapely.get_coordinates(decoded, include_z=True, include_m=True).tolist(),
                         [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0],
                          [9.0, 10.0, 11.0, 12.0], [13.0, 14.0, 15.0, 16.0]])

    def test_polyline_geometry_drift_is_refused(self):
        base = _fixture_geometry(FIXTURE_OBJECTIDS[0])
        cases = {
            "unexpected_source_geometry_members": {**base, "curves": []},
            "source_geometry_z_or_m_flag_missing": {"hasZ": True, "hasM": False,
                                                    "paths": base["paths"]},
            "source_polyline_paths_missing": {"hasZ": True, "hasM": True, "paths": []},
            "source_polyline_path_too_short": {"hasZ": True, "hasM": True,
                                               "paths": [base["paths"][0][:1]]},
            "source_polyline_ordinate_contract_drift": {
                "hasZ": True, "hasM": True,
                "paths": [[vertex[:3] for vertex in base["paths"][0]]]},
            "source_polyline_ordinate_not_numeric": {
                "hasZ": True, "hasM": True,
                "paths": [[["NaN"] + vertex[1:] for vertex in base["paths"][0]]]},
            "source_polyline_ordinate_not_finite": {
                "hasZ": True, "hasM": True,
                "paths": [[vertex[:2] + [float("inf"), vertex[3]]
                           for vertex in base["paths"][0]]]},
            "source_coordinate_range_drift": {
                "hasZ": True, "hasM": True,
                "paths": [[[200.0] + vertex[1:] for vertex in base["paths"][0]]]},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.NhdFlowlineError, expected_error):
                    src._polyline_wkb(geometry)

    def test_disk_floor_refuses_before_any_fetch(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        fetches = {"count": 0}
        real_send = httpx.Client.send

        def counting_send(client, request, **kwargs):
            fetches["count"] += 1
            return real_send(client, request, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with patch.object(src.shutil, "disk_usage",
                                  return_value=SimpleNamespace(free=src.MIN_FREE_BYTES - 1)), \
                        patch.object(httpx.Client, "send", counting_send):
                    with self.assertRaisesRegex(src.NhdFlowlineError,
                                                "disk_floor_before_acquisition"):
                        src.capture_nhd_flowline(_staging(root), version="floor-run",
                                                 capture_directory=str(root / "capture"))
            self.assertEqual(fetches["count"], 0)
            self.assertFalse((root / "capture").exists())
            self.assertFalse(any((root / "staging").iterdir()))

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_flowline(storage, version="fixture-run",
                                                 capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_flowline(storage, version="fixture-run-2",
                                                  capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(),
                             list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("id3dhp").to_pylist(),
                             [row["attributes"]["id3dhp"] for row in rows])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["LineString ZM", "MultiLineString ZM"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"]["baseCRS"]["name"]
                             if "baseCRS" in geo_metadata["columns"]["geometry"]["crs"]
                             else geo_metadata["columns"]["geometry"]["crs"].get("name", ""),
                             "WGS 84")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport, page_size=2):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_flowline, nhd_flowline_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__nhd_flowline__nhd_flowline": {"config": {
                            "capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    check = checks["nhd_flowline_complete"]
                    self.assertTrue(check.passed)
                    candidate = result.output_for_node("ingest__nhd_flowline__nhd_flowline")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_nhd_flowline(_staging(root), version="seal-run",
                                                  capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertTrue(sealed["baseline_pending_complete_capture"])
            self.assertEqual(sealed["pagination"]["mode"], "objectid_keyset_asc")
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov",
                                         "hydro.nationalmap.gov", "www.usgs.gov")
                                for host in hosts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_seal_and_reuse_stream_parquet_without_reading_it_into_memory(self):
        """The national parquet is sealed and verified as a streamed file.

        Reading the candidate parquet through read_bytes would buffer the
        complete national geometry population (tens of GB) in memory during the
        seal, contradicting this module's bounded streaming contract.
        """
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_flowline(storage, version="stream-seal-run",
                                                 capture_directory=str(capture_root))
                parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
                real_read_bytes = Path.read_bytes

                def refuse_parquet_read_bytes(path: Path):
                    if path == parquet_path:
                        raise AssertionError("candidate parquet was read into memory")
                    return real_read_bytes(path)

                with patch.object(Path, "read_bytes", refuse_parquet_read_bytes):
                    reused = src.capture_nhd_flowline(storage, version="stream-seal-run-2",
                                                      capture_directory=str(capture_root))
                    fresh = src.capture_nhd_flowline(_staging(root / "b"),
                                                     version="stream-seal-run",
                                                     capture_directory=str(capture_root))
            self.assertTrue(reused["reused"])
            self.assertFalse(fresh["reused"])
            self.assertEqual(fresh["version"], first["version"])
            self.assertEqual(fresh["feature_count"], FIXTURE_COUNT)
            sealed = (f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{first['version']}"
                      f"/geoparquet/{src.DATASET_SLUG}.parquet")
            self.assertEqual((root / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())
            self.assertEqual((root / "b" / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())

    def test_stream_validate_extrema_are_running_and_memory_bounded(self):
        """Bounds stay four running extrema; per-vertex retention is refused.

        Holding both ordinate floats for every vertex of the 30,251,533-feature
        population would retain gigabytes during the streaming validation, so
        this regression walks many vertex-heavy pages and requires the traced
        peak to stay below the accumulation the old list layout needed.
        """
        import tracemalloc

        pages = 15
        features_per_page = 2
        vertices_per_feature = 6000
        count = pages * features_per_page

        def geometry(row: dict) -> dict:
            index = row["attributes"]["OBJECTID"] - 1
            return {"hasZ": True, "hasM": True, "paths": [
                [[-170.0 + index * 0.1 + offset * 1e-6,
                  80.0 - index * 0.1 - offset * 1e-6,
                  1.0 + offset * 0.5, 2.0 + offset * 0.25]
                 for offset in range(vertices_per_feature)]]}

        rows = []
        for oid in range(1, count + 1):
            attrs = dict(_fixture_attrs(FIXTURE_OBJECTIDS[0]))
            attrs["OBJECTID"] = oid
            rows.append({"attributes": attrs, "geometry": None})
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, geometry=geometry)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2), \
                    patch.object(src, "EXPECTED_COUNT", count):
                src.fetch_nhd_flowline_pages(capture_root)
                tracemalloc.start()
                try:
                    extracted = src._stream_validate(capture_root, parquet_sink=None)
                finally:
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
            last = count - 1
            self.assertEqual(extracted["observed_bounds"], {
                "xmin": -170.0,
                "ymin": 80.0 - last * 0.1 - (vertices_per_feature - 1) * 1e-6,
                "xmax": -170.0 + last * 0.1 + (vertices_per_feature - 1) * 1e-6,
                "ymax": 80.0,
            })
            accumulation_bytes = count * vertices_per_feature * 2 * 32
            self.assertLess(peak, accumulation_bytes)

    def test_packaged_identity_first_seal_and_reuse_validate_for_real(self):
        """A Git-free installed tree must seal and reuse with real validation.

        The shipped runtime has no .git, so processing_identity returns a
        packaged-sha256 identity; validate_construction_code_identity must
        receive the full identity on both the first-seal and the reuse path, or
        it rejects the packaged commit as construction_invalid_commit.
        """
        import shutil as shutil_module

        from dagster_hifld.construction_provenance import validate_construction_code_identity

        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tree = root / "installed"
            (tree / "src").mkdir(parents=True)
            shutil_module.copytree(Path(src.__file__).parents[1], tree / "src" / "dagster_hifld",
                                   ignore=shutil_module.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil_module.copy2(Path(src.__file__).parents[3] / name, tree / name)
            with fixture_mode(schema_sha, transport, page_size=2, real_identity=True), \
                    patch.object(src, "__file__", str(tree / "src" / "dagster_hifld"
                                                      / "sources" / "nhd_flowline.py")):
                storage = _staging(root)
                first = src.capture_nhd_flowline(storage, version="packaged-run",
                                                 capture_directory=str(root / "capture"))
                identity = src._code_identity()
                self.assertEqual(identity["code_identity_scheme"],
                                 "sha256-packaged-dagster-hifld-v1")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                second = src.capture_nhd_flowline(storage, version="packaged-run-2",
                                                  capture_directory=str(root / "capture"))
                self.assertTrue(second["reused"])
                self.assertEqual(first["version"], second["version"])
                validate_construction_code_identity(tree, identity["code_commit"],
                                                    identity["files"], identity=identity)

    def test_transient_error_page_is_witnessed_and_resume_refetches(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        base = make_responder(rows)
        injected = {"error_sent": False}

        def responder(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if ("where" in query and query["where"][0].startswith("OBJECTID>")
                    and "returnCountOnly" not in query and not injected["error_sent"]):
                injected["error_sent"] = True
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 429, "message": "busy"}}),
                    headers={"content-type": "application/json"})
            return base.handler(request)

        transport = httpx.MockTransport(responder)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.NhdFlowlineError, "source_page_error"):
                    src.fetch_nhd_flowline_pages(capture_root)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                self.assertFalse((capture_root / "page_000000.body").exists())
                result = src.fetch_nhd_flowline_pages(capture_root)
                self.assertTrue(result["complete"])
                storage = _staging(root)
                sealed = src.capture_nhd_flowline(storage, version="witness-run",
                                                  capture_directory=str(capture_root))
                self.assertEqual(sealed["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))

    def test_resume_reuses_retained_pages_and_discards_torn_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_nhd_flowline(storage, version="resume-run",
                                         capture_directory=str(capture_root))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(len(state["data_pages"]), 3)
                # Simulate an interruption after the final data page: drop the tail witness.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").unlink()
                state["tail"] = None
                (capture_root / "capture_state.json").write_bytes(src._canonical(state) + b"\n")
                fetches = {"count": 0}
                real_send = httpx.Client.send

                def counting_send(client, request, **kwargs):
                    fetches["count"] += 1
                    return real_send(client, request, **kwargs)

                with patch.object(httpx.Client, "send", counting_send):
                    result = src.fetch_nhd_flowline_pages(capture_root)
                self.assertTrue(result["complete"])
                self.assertLessEqual(fetches["count"], 6)
                self.assertTrue((capture_root / "page_tail.body").exists())
                # A torn retained response is never trusted without a fetch client.
                (capture_root / "count_after.body").write_bytes(b'{"count":999999999}')
                with self.assertRaisesRegex(src.NhdFlowlineError,
                                            "retained_capture_receipt_invalid"):
                    src._retained_or_fetch(None, capture_root, "count_after", src._count_url())
                # With a fetch client the torn response is discarded and refetched.
                with patch.object(httpx.Client, "send", counting_send):
                    src.fetch_nhd_flowline_pages(capture_root)
                self.assertTrue((capture_root / "page_tail.body").exists())

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
            "exact_3dhp_flowline_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolygon"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": False}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": dict(src.NATIVE_LAYER_SR)}}),
            "source_page_transfer_truncated": dict(
                geometry=lambda row: _fixture_geometry(row["attributes"]["OBJECTID"]),
                truncate_short_page=True),
            "unexpected_source_geometry_members": dict(
                geometry=lambda row: {**_fixture_geometry(row["attributes"]["OBJECTID"]),
                                      "curves": []}),
            "source_coordinate_range_drift": dict(
                geometry=lambda row: {"hasZ": True, "hasM": True, "paths": [
                    [[200.0, vertex[1], vertex[2], vertex[3]] for vertex in
                     _fixture_geometry(row["attributes"]["OBJECTID"])["paths"][0]]]}),
            "source_object_ids_not_strictly_increasing": dict(rows=[
                dict(rows[0]), dict(rows[0])], count=FIXTURE_COUNT),
            "source_object_id_type_changed": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "OBJECTID": None}},
                *rows[1:]]),
            "source_native_type_changed:gnisid": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "gnisid": "694899"}},
                *rows[1:]]),
            "source_native_type_changed:lengthkm": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "lengthkm": float("nan")}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "id3dhp": "1"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    truncate_short_page = kwargs.pop("truncate_short_page", False)
                    case_rows = kwargs.pop("rows", rows)
                    base_transport = make_responder(case_rows, **kwargs)

                    def responder(request: httpx.Request, _base=base_transport,
                                  _truncate=truncate_short_page) -> httpx.Response:
                        query = parse_qs(urlparse(str(request.url)).query)
                        response = _base.handler(request)
                        if _truncate and "where" in query \
                                and query["where"][0].startswith("OBJECTID>") \
                                and "returnCountOnly" not in query:
                            payload = json.loads(response.read())
                            if len(payload["features"]) < src.PAGE_SIZE:
                                payload["exceededTransferLimit"] = True
                            return httpx.Response(200, stream=JSONStream(payload),
                                                  headers={"content-type": "application/json"})
                        return response

                    transport = httpx.MockTransport(responder)
                    with fixture_mode(schema_sha, transport, page_size=2):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.NhdFlowlineError, expected_error):
                            src.capture_nhd_flowline(storage, version="drift-run",
                                                     capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_null_geometry_contract_is_enforced(self):
        rows = [{**row, "geometry": None} for row in _fixture_rows()]
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.NhdFlowlineError,
                                            "source_null_geometry_count_drift"):
                    src.capture_nhd_flowline(_staging(root), version="null-geometry-run",
                                             capture_directory=str(root / "capture"))

    def test_pinned_baseline_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_nhd_flowline(storage, version="pin-run",
                                         capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    again = src.capture_nhd_flowline(storage, version="pin-run-verify",
                                                     capture_directory=str(capture_root))
                    self.assertTrue(again["reused"])
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.NhdFlowlineError,
                                                "source_id_roster_drift"):
                        src.capture_nhd_flowline(storage, version="pin-run-drift",
                                                 capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertEqual(failure["reason_code"], "source_id_roster_drift")


if __name__ == "__main__":
    unittest.main()
